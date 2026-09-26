/**
 * @import {ChunkPlan, ColumnDecoder, DataReader, DecodedArray, GroupPlan, PageLocation, ParquetReadOptions, QueryPlan, SchemaTree} from '../src/types.js'
 */

import { parquetHeader } from './column.js'
import { DEFAULT_PARSERS, convert, convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDataPageV2 } from './datapage.js'
import { readOffsetIndex } from './indexes.js'
import { readPlain } from './plain.js'
import { getMaxRepetitionLevel, getSchemaPath } from './schema.js'

/**
 * Read selected physical pages for column views. Row-oriented reads keep
 * their existing fetch/decode path: a shared async fetch layer would add a
 * promise boundary to every page-selected column in that path.
 *
 * @param {ParquetReadOptions} options
 * @param {QueryPlan} plan
 * @param {GroupPlan} groupPlan
 * @returns {{pathInSchema: string[], schemaPath: SchemaTree[], pages: Promise<{pages: import('../src/types.js').ColumnLevelPage[], rowStart: number}>}[]}
 */
export function readRowGroupPages(options, { metadata }, groupPlan) {
  return groupPlan.chunks.map(chunk => {
    const { path_in_schema: pathInSchema } = chunk.columnMetadata
    const schemaPath = getSchemaPath(metadata.schema, pathInSchema)
    const columnDecoder = {
      pathInSchema,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      ...options,
      ...chunk.columnMetadata,
      parsers: { ...DEFAULT_PARSERS, ...options.parsers },
    }
    const { startByte, endByte } = chunk.range
    /** @returns {Promise<{pages: import('../src/types.js').ColumnLevelPage[], rowStart: number}>} */
    async function selectedPages() {
      const locations = 'pageLocations' in chunk ? chunk.pageLocations :
        'offsetIndex' in chunk ? readOffsetIndex({
          view: new DataView(await options.file.slice(chunk.offsetIndex.startByte, chunk.offsetIndex.endByte)),
          offset: 0,
        }).page_locations : undefined
      if (locations) {
        const repeated = getMaxRepetitionLevel(schemaPath) > 0
        const { view, skipped } = await fetchColumnViewPages(options, groupPlan, chunk, locations, repeated)
        return { pages: readColumnPages({ view, offset: 0 }, columnDecoder), rowStart: groupPlan.groupStart + skipped }
      }
      const view = new DataView(await options.file.slice(startByte, endByte))
      return { pages: readColumnPages({ view, offset: 0 }, columnDecoder), rowStart: groupPlan.groupStart }
    }
    return { pathInSchema, schemaPath, pages: selectedPages() }
  })
}

/**
 * Fetch pages for a column view, including earlier pages only when the offset
 * index says they start in the same row. Well-formed indexes need no lookback.
 *
 * @param {ParquetReadOptions} options
 * @param {GroupPlan} groupPlan
 * @param {ChunkPlan} chunk
 * @param {PageLocation[]} pages
 * @param {boolean} repeated
 * @returns {Promise<{view: DataView, skipped: number}>}
 */
async function fetchColumnViewPages(options, groupPlan, chunk, pages, repeated) {
  const { data_page_offset, dictionary_page_offset } = chunk.columnMetadata
  const { selectStart, selectEnd } = groupPlan
  let { startByte, endByte } = chunk.range
  let skipped = -1
  let firstPage = -1
  const hasDict = pages.length > 0 && (dictionary_page_offset || data_page_offset < pages[0].offset)
  for (let i = 0; i < pages.length; i++) {
    const page = pages[i]
    const pageStart = Number(page.first_row_index)
    const pageEnd = i + 1 < pages.length
      ? Number(pages[i + 1].first_row_index)
      : groupPlan.groupRows
    if (skipped < 0 && pageEnd > selectStart) {
      startByte = Number(page.offset)
      skipped = pageStart
      firstPage = i
    }
    if (pageStart < selectEnd) {
      endByte = Number(page.offset) + page.compressed_page_size
    }
  }
  if (skipped < 0) skipped = 0
  if (repeated && firstPage > 0) {
    while (firstPage > 0 && Number(pages[firstPage - 1].first_row_index) === skipped) firstPage--
    startByte = Number(pages[firstPage].offset)
    skipped = Number(pages[firstPage].first_row_index)
  }
  /** @type {DataView} */
  let view
  if (hasDict && skipped) {
    const dictLength = Number(pages[0].offset) - chunk.range.startByte
    const [dictBuffer, dataBuffer] = await Promise.all([
      options.file.slice(chunk.range.startByte, Number(pages[0].offset)),
      options.file.slice(startByte, endByte),
    ])
    const combined = new Uint8Array(dictLength + dataBuffer.byteLength)
    combined.set(new Uint8Array(dictBuffer, 0, dictLength))
    combined.set(new Uint8Array(dataBuffer), dictLength)
    view = new DataView(combined.buffer)
  } else if (hasDict) {
    view = new DataView(await options.file.slice(chunk.range.startByte, endByte))
  } else {
    view = new DataView(await options.file.slice(startByte, endByte))
  }
  return { view, skipped }
}

/**
 * Decode physical pages for a column view without assembling nested rows.
 * Keep the existing readPage path independent so row-oriented reads retain
 * their page-by-page behavior and cost.
 *
 * @param {DataReader} reader
 * @param {ColumnDecoder} columnDecoder
 * @returns {import('../src/types.js').ColumnLevelPage[]}
 */
function readColumnPages(reader, columnDecoder) {
  const { type, element, codec, compressors } = columnDecoder
  /** @type {DecodedArray | undefined} */
  let dictionary
  /** @type {import('../src/types.js').ColumnLevelPage[]} */
  const pages = []
  while (reader.offset < reader.view.byteLength - 1) {
    const header = parquetHeader(reader)
    const compressedBytes = new Uint8Array(
      reader.view.buffer, reader.view.byteOffset + reader.offset, header.compressed_page_size
    )
    reader.offset += header.compressed_page_size

    if (header.type === 'DICTIONARY_PAGE') {
      const diph = header.dictionary_page_header
      if (!diph) throw new Error('parquet dictionary page header is undefined')
      const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), codec, compressors)
      const dictReader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 }
      dictionary = convert(readPlain(dictReader, type, diph.num_values, element.type_length), columnDecoder)
    } else if (header.type === 'DATA_PAGE') {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')
      const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), codec, compressors)
      const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, columnDecoder)
      pages.push({
        values: convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder),
        definitionLevels: definitionLevels || [],
        repetitionLevels: repetitionLevels || [],
      })
    } else if (header.type === 'DATA_PAGE_V2') {
      const daph = header.data_page_header_v2
      if (!daph) throw new Error('parquet data page header v2 is undefined')
      const { definitionLevels, repetitionLevels, dataPage } = readDataPageV2(compressedBytes, header, columnDecoder)
      pages.push({
        values: convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder),
        definitionLevels: definitionLevels || [],
        repetitionLevels: repetitionLevels || [],
      })
    } else {
      throw new Error(`parquet unsupported page type: ${header.type}`)
    }
  }
  return pages
}
