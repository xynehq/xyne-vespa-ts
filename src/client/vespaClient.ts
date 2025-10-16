import type {
  VespaAutocompleteResponse,
  VespaFile,
  VespaMail,
  VespaSearchResponse,
  VespaUser,
  VespaGetResult,
  VespaEvent,
  VespaUserQueryHistory,
  VespaSchema,
  VespaMailAttachment,
  VespaChatContainer,
  Inserts,
  Span,
} from "../types"
import {
  chatContainerSchema,
  chatMessageSchema,
  chatUserSchema,
  mailSchema,
} from "../types"
import { getErrorMessage } from "../utils"
import { handleVespaGroupResponse } from "../mappers"
import type { ILogger } from "../types"
import { YqlBuilder } from "../yql/yqlBuilder"
import { contains, inArray, matches, or, sameElement } from "../yql"

// Define EntityCounts type
export interface EntityCounts {
  [entity: string]: number
}

// Define AppEntityCounts type
export interface AppEntityCounts {
  [app: string]: EntityCounts
}

// Console fallback logger
const consoleLogger: ILogger = {
  info: (message: string, ...args: any[]) =>
    console.info(`[INFO] ${message}`, ...args),
  error: (message: string | Error, ...args: any[]) => {
    const msg = message instanceof Error ? message.message : message
    console.error(`[ERROR] ${msg}`, ...args)
  },
  warn: (message: string, ...args: any[]) =>
    console.warn(`[WARN] ${message}`, ...args),
  debug: (message: string, ...args: any[]) =>
    console.debug(`[DEBUG] ${message}`, ...args),
  child: (metadata: Record<string, any>) => consoleLogger,
}
type VespaConfigValues = {
  namespace?: string
  schema?: VespaSchema
  cluster?: string
}

class VespaClient {
  private maxRetries: number
  private retryDelay: number
  private vespaEndpoint: string
  private logger: ILogger

  constructor(
    endpoint?: string,
    logger?: ILogger,
    config?: {
      vespaMaxRetryAttempts?: number
      vespaRetryDelay?: number
      vespaBaseHost?: string
    },
  ) {
    this.logger = logger || consoleLogger
    this.maxRetries = config?.vespaMaxRetryAttempts || 3
    this.retryDelay = config?.vespaRetryDelay || 1000 // milliseconds
    this.vespaEndpoint =
      endpoint || `http://${config?.vespaBaseHost || "localhost"}:8080`
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
  private async fetchWithRetry(
    url: string,
    options: RequestInit,
    retryCount = 0,
  ): Promise<Response> {
    const nonRetryableStatusCodes = [404]
    try {
      const response = await fetch(url, options)
      if (!response.ok) {
        // Don't need to retry for non-retryable status codes
        if (nonRetryableStatusCodes.includes(response.status)) {
          throw new Error(
            `Non-retryable error: ${response.status} ${response.statusText}`,
          )
        }

        // Retry for 429 (Too Many Requests) or 5xx errors
        if (
          (response.status === 429 || response.status >= 500) &&
          retryCount < this.maxRetries
        ) {
          this.logger.info("retrying due to status: ", response.status)
          await this.delay(this.retryDelay * Math.pow(2, retryCount))
          return this.fetchWithRetry(url, options, retryCount + 1)
        }
      }

      return response
    } catch (error) {
      const errorMessage = getErrorMessage(error)

      if (
        retryCount < this.maxRetries &&
        !errorMessage.includes("Non-retryable error")
      ) {
        await this.delay(this.retryDelay * Math.pow(2, retryCount)) // Exponential backoff
        return this.fetchWithRetry(url, options, retryCount + 1)
      }
      throw error
    }
  }

  async search<T>(payload: any): Promise<T> {
    const url = `${this.vespaEndpoint}/search/`

    try {
      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(
          `Vespa search failed - Status: ${response.status}, StatusText: ${errorText}`,
        )
        this.logger.error(`Vespa error body: ${errorBody}`)
        throw new Error(
          `Failed to fetch documents in searchVespa: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = await response.json()
      return result as T
    } catch (error: any) {
      this.logger.error(
        `VespaClient.search error: ${JSON.stringify(error)}`,
        error,
      )
      throw new Error(`Vespa search error: ${error.message}`)
    }
  }
  private async fetchDocumentBatch(
    schema: VespaSchema,
    options: VespaConfigValues,
    limit: number,
    offset: number,
    email: string,
  ): Promise<any[]> {
    const yqlQuery = `select * from sources ${schema} where true`
    const searchPayload = {
      yql: yqlQuery,
      hits: limit,
      offset,
      timeout: "10s",
    }

    const response = await this.search<VespaSearchResponse>(searchPayload)
    return (response.root?.children || []).map((doc) => {
      // Use optional chaining and nullish coalescing to safely extract fields
      const { matchfeatures, ...fieldsWithoutMatch } = doc.fields as any
      return fieldsWithoutMatch
    })
  }

  async getAllDocumentsParallel(
    schema: VespaSchema,
    options: VespaConfigValues,
    concurrency: number = 3,
    email: string,
  ): Promise<any[]> {
    // First get document count
    const countResponse = await this.getDocumentCount(schema, options, email)
    const totalCount = countResponse?.root?.fields?.totalCount || 0

    if (totalCount === 0) return []

    // Calculate optimal batch size and create batch tasks
    const batchSize = 350
    const tasks = []

    for (let offset = 0; offset < totalCount; offset += batchSize) {
      tasks.push(() =>
        this.fetchDocumentBatch(schema, options, batchSize, offset, email),
      )
    }

    // Run tasks with concurrency limit
    const pLimit = (await import("p-limit")).default
    const limit = pLimit(concurrency)
    const results = await Promise.all(tasks.map((task) => limit(task)))

    // Flatten results
    return results.flat()
  }

  async deleteAllDocuments(options: VespaConfigValues): Promise<void> {
    const { cluster, namespace, schema } = options
    // Construct the DELETE URL
    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid?selection=true&cluster=${cluster}`

    try {
      const response: Response = await this.fetchWithRetry(url, {
        method: "DELETE",
      })

      if (response.ok) {
        this.logger.info("All documents deleted successfully.")
      } else {
        const errorText = response.statusText
        throw new Error(
          `Failed to delete documents: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }
    } catch (error) {
      this.logger.error(
        `Error deleting documents:, ${error} ${(error as Error).stack}`,
        error,
      )
      throw new Error(`Vespa delete error: ${error}`)
    }
  }

  async insertDocument(
    document: VespaFile,
    options: VespaConfigValues,
  ): Promise<void> {
    try {
      const url = `${this.vespaEndpoint}/document/v1/${options.namespace}/${options.schema}/docid/${document.docId}`
      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: document }),
      })

      if (!response.ok) {
        // Using status text since response.text() return Body Already used Error
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(`Vespa error: ${errorBody}`)
        throw new Error(
          `Failed to  insert document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }
      const data = await response.json()

      if (response.ok) {
        // this.logger.info(`Document ${document.docId} inserted successfully`)
      } else {
        this.logger.error(`Error inserting document ${document.docId}`)
      }
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error inserting document ${document.docId}: ${errMessage}`,
        error,
      )
      throw new Error(
        `Error inserting document ${document.docId}: ${errMessage}`,
      )
    }
  }

  async insert(document: Inserts, options: VespaConfigValues): Promise<void> {
    try {
      const url = `${this.vespaEndpoint}/document/v1/${options.namespace}/${options.schema}/docid/${document.docId}`
      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: document }),
      })

      if (!response.ok) {
        // Using status text since response.text() return Body Already used Error
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(`Vespa error: ${errorBody}`)
        throw new Error(
          `Failed to  insert document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = await response.json()

      if (response.ok) {
        this.logger.info(`Document ${document.docId} inserted successfully`)
      } else {
      }
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error inserting document ${document.docId}: ${errMessage} ${(error as Error).stack}`,
        error,
      )
      throw new Error(
        `Error inserting document ${document.docId}: ${errMessage} ${(error as Error).stack}`,
      )
    }
  }

  async insertUser(user: VespaUser, options: VespaConfigValues): Promise<void> {
    try {
      const url = `${this.vespaEndpoint}/document/v1/${options.namespace}/${options.schema}/docid/${user.docId}`
      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: user }),
      })

      const data = await response.json()

      if (response.ok) {
        // this.logger.info(`Document ${user.docId} inserted successfully:`, data)
      } else {
        this.logger.error(`Error inserting user ${user.docId}: ${data}`, data)
      }
    } catch (error) {
      const errorMessage = getErrorMessage(error)
      this.logger.error(
        `Error inserting user ${user.docId}:`,
        errorMessage,
        error,
      )
      throw new Error(`Error inserting user ${user.docId}: ${errorMessage}`)
    }
  }

  async autoComplete<T>(searchPayload: T): Promise<VespaAutocompleteResponse> {
    try {
      const url = `${this.vespaEndpoint}/search/`

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(searchPayload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(
          `AutoComplete failed - Status: ${response.status}, StatusText: ${errorText}`,
        )
        this.logger.error(`AutoComplete error body: ${errorBody}`)
        throw new Error(
          `Failed to perform autocomplete search: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = (await response.json()) as VespaAutocompleteResponse
      return data
    } catch (error) {
      this.logger.error(`VespaClient.autoComplete error:`, error)
      throw new Error(
        `Error performing autocomplete search:, ${error} ${(error as Error).stack} `,
      )
      // TODO: instead of null just send empty response
      throw error
    }
  }

  async groupSearch<T>(payload: T): Promise<AppEntityCounts> {
    try {
      const url = `${this.vespaEndpoint}/search/`
      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })
      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to fetch documents in groupVespaSearch: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = (await response.json()) as VespaSearchResponse
      return handleVespaGroupResponse(data)
    } catch (error) {
      this.logger.error(
        `Error performing search groupVespaSearch:, ${error} - ${(error as Error).stack}`,
        error,
      )
      throw new Error(
        `Error performing search groupVespaSearch:, ${error} - ${(error as Error).stack}`,
      )
    }
  }

  async getDocumentCount(
    schema: VespaSchema,
    options: VespaConfigValues,
    email: string,
  ) {
    try {
      // Encode the YQL query to ensure it's URL-safe
      const yql = encodeURIComponent(
        `select * from sources ${schema} where uploadedBy contains '${email}'`,
      )
      // Construct the search URL with necessary query parameters
      const url = `${this.vespaEndpoint}/search/?yql=${yql}&hits=0&cluster=${options.cluster}`
      const response: Response = await this.fetchWithRetry(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to fetch document count: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = (await response.json()) as any

      // Extract the total number of hits from the response
      const totalCount = data?.root?.fields?.totalCount

      if (typeof totalCount === "number") {
        this.logger.info(
          `Total documents in schema '${schema}' within namespace '${options.namespace}' and cluster '${options.cluster}': ${totalCount}`,
        )
        return data
      } else {
        this.logger.error(`Unexpected response structure:', ${data}`)
      }
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(`Error retrieving document count: ${errMessage}`)
      throw new Error(`Error retrieving document count: ${errMessage}`)
    }
  }

  async getDocument(
    options: VespaConfigValues & { docId: string },
  ): Promise<VespaGetResult> {
    const { docId, namespace, schema } = options
    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid/${docId}`
    try {
      const response = await this.fetchWithRetry(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      })
      if (!response.ok) {
        const errorText = response.statusText
        const errorBody = await response.text()
        throw new Error(
          `Failed to fetch document: ${response.status} ${response.statusText} - ${errorBody}`,
        )
      }

      const document = (await response.json()) as VespaGetResult
      return document
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(`Error fetching document docId: ${docId} - ${errMessage}`)
    }
  }

  async getDocumentsByOnlyDocIds(
    options: VespaConfigValues & {
      docIds: string[]
      generateAnswerSpan: Span
      yql: string
    },
  ): Promise<VespaSearchResponse> {
    const { docIds, generateAnswerSpan, yql } = options
    const url = `${this.vespaEndpoint}/search/`

    try {
      const payload = {
        yql: yql,
        hits: docIds?.length,
        maxHits: docIds?.length,
      }

      generateAnswerSpan.setAttribute("vespaPayload", JSON.stringify(payload))

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse
      return result
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(`Error fetching documents: ${errMessage}`)
    }
  }

  async updateDocumentPermissions(
    permissions: string[],
    options: VespaConfigValues & { docId: string },
  ): Promise<void> {
    const { docId, namespace, schema } = options

    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid/${docId}`
    try {
      const response = await this.fetchWithRetry(url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          fields: {
            permissions: { assign: permissions },
          },
        }),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to update document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      this.logger.info(
        `Successfully updated permissions in schema ${schema} for document ${docId}.`,
      )
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error updating permissions in schema ${schema} for document ${docId}:`,
        error,
        errMessage,
      )
      throw new Error(
        `Error updating permissions in schema ${schema} for document ${docId}: ${errMessage}`,
      )
    }
  }

  async updateCancelledEvents(
    cancelledInstances: string[],
    options: VespaConfigValues & { docId: string },
  ): Promise<void> {
    const { docId, namespace, schema } = options
    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid/${docId}`
    try {
      const response = await fetch(url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          fields: {
            cancelledInstances: { assign: cancelledInstances },
          },
        }),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to update document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      this.logger.info(
        `Successfully updated event instances in schema ${schema} for document ${docId}.`,
      )
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error updating event instances in schema ${schema} for document ${docId}:`,
        error,
        errMessage,
      )
      throw new Error(
        `Error updating event instances in schema ${schema} for document ${docId}: ${errMessage}`,
      )
    }
  }

  async updateDocument(
    updatedFields: Record<string, any>,
    options: VespaConfigValues & { docId: string },
  ): Promise<void> {
    const { docId, namespace, schema } = options

    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid/${docId}`
    let fields: string[] = []
    try {
      const updateObject = Object.entries(updatedFields).reduce(
        (prev, [key, value]) => {
          // for logging
          fields.push(key)
          prev[key] = { assign: value }
          return prev
        },
        {} as Record<string, any>,
      )
      const response = await this.fetchWithRetry(url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          fields: updateObject,
        }),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to update document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      this.logger.info(
        `Successfully updated ${fields} in schema ${schema} for document ${docId}.`,
      )
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error updating ${fields} in schema ${schema} for document ${docId}:`,
        error,
        errMessage,
      )
      throw new Error(
        `Error updating ${fields} in schema ${schema} for document ${docId}: ${errMessage}`,
      )
    }
  }

  async deleteDocument(
    options: VespaConfigValues & { docId: string },
  ): Promise<void> {
    const { docId, namespace, schema } = options // Extract namespace and schema again
    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid/${docId}` // Revert to original URL construction
    try {
      const response = await this.fetchWithRetry(url, {
        method: "DELETE",
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to delete document: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      this.logger.info(`Document ${docId} deleted successfully.`)
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error deleting document ${docId}:  ${errMessage}`,
        error,
      )
      throw new Error(`Error deleting document ${docId}:  ${errMessage}`)
    }
  }

  async ifDocumentsExistInChatContainer(
    docIds: string[],
  ): Promise<
    Record<
      string,
      { exists: boolean; updatedAt: number | null; permissions: string[] }
    >
  > {
    // If no docIds are provided, return an empty record
    if (!docIds.length) {
      return {}
    }

    // Set a reasonable batch size for each query
    const BATCH_SIZE = 500
    let existenceMap: Record<
      string,
      { exists: boolean; updatedAt: number | null; permissions: string[] }
    > = {}

    // Process docIds in batches
    for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
      const batchDocIds = docIds.slice(i, i + BATCH_SIZE)
      this.logger.info(
        `Processing batch ${Math.floor(i / BATCH_SIZE) + 1} with ${batchDocIds.length} document IDs`,
      )

      // Construct the YQL query for this batch
      const yql = YqlBuilder.create({ requirePermissions: false })
        .select(["docId", "updatedAt", "permissions"])
        .from(chatContainerSchema)
        .where(inArray("docId", batchDocIds))
        .build()

      const url = `${this.vespaEndpoint}/search/`

      try {
        const payload = {
          yql,
          hits: batchDocIds.length,
          maxHits: batchDocIds.length + 1,
        }

        const response = await this.fetchWithRetry(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        })

        if (!response.ok) {
          const errorText = response.statusText
          throw new Error(
            `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
          )
        }

        const result = (await response.json()) as VespaSearchResponse

        // Extract found documents with their docId, updatedAt, and permissions
        const foundDocs =
          result.root?.children?.map((hit: any) => ({
            docId: hit.fields.docId as string,
            updatedAt: hit.fields.updatedAt as number | undefined,
            permissions: hit.fields.permissions as string[] | undefined,
          })) || []

        // Add to the result map for this batch
        const batchExistenceMap = batchDocIds.reduce(
          (acc, id) => {
            const foundDoc = foundDocs.find(
              (doc: { docId: string }) => doc.docId === id,
            )
            acc[id] = {
              exists: !!foundDoc,
              updatedAt: foundDoc?.updatedAt ?? null,
              permissions: foundDoc?.permissions ?? [], // Empty array if not found or no permissions
            }
            return acc
          },
          {} as Record<
            string,
            { exists: boolean; updatedAt: number | null; permissions: string[] }
          >,
        )

        // Merge the batch results into the overall map
        existenceMap = { ...existenceMap, ...batchExistenceMap }
      } catch (error) {
        const errMessage = getErrorMessage(error)
        this.logger.error(
          `Error checking batch of chat container documents existence: ${errMessage}`,
          error,
        )
        throw error
      }
    }

    return existenceMap
  }
  // TODO: Add pagination if docId's are more than
  // max hits and merge the finaly Record
  async ifDocumentsExist(
    docIds: string[],
  ): Promise<Record<string, { exists: boolean; updatedAt: number | null }>> {
    // Construct the YQL query
    const yql = YqlBuilder.create({ requirePermissions: false })
      .select(["docId", "updatedAt"])
      .from("*")
      .where(inArray("docId", docIds))
      .build()
    const url = `${this.vespaEndpoint}/search/`

    try {
      const payload = {
        yql,
        hits: docIds.length,
        maxHits: docIds.length + 1,
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse

      // Extract found documents with their docId and updatedAt
      const foundDocs =
        result.root?.children?.map((hit: any) => ({
          docId: hit.fields.docId as string,
          updatedAt: hit.fields.updatedAt as number | undefined, // undefined if not present
        })) || []

      // Build the result map
      const existenceMap = docIds.reduce(
        (acc, id) => {
          const foundDoc = foundDocs.find(
            (doc: { docId: string }) => doc.docId === id,
          )
          acc[id] = {
            exists: !!foundDoc,
            updatedAt: foundDoc?.updatedAt ?? null, // null if not found or no updatedAt
          }
          return acc
        },
        {} as Record<string, { exists: boolean; updatedAt: number | null }>,
      )

      return existenceMap
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error checking documents existence:  ${errMessage}`,
        error,
      )
      throw error
    }
  }

  async ifMailDocumentsExist(mailIds: string[]): Promise<
    Record<
      string,
      {
        docId: string
        exists: boolean
        updatedAt: number | null
        userMap: Record<string, string>
      }
    >
  > {
    // Construct the YQL query
    const yql = YqlBuilder.create({ requirePermissions: false })
      .select(["docId", "mailId", "updatedAt", "userMap"])
      .from("mail")
      .where(inArray("mailId", mailIds))
      .build()

    const url = `${this.vespaEndpoint}/search/`

    try {
      const payload = {
        yql,
        hits: mailIds.length,
        maxHits: mailIds.length + 1,
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse
      // Extract found documents with their mailId and updatedAt
      const foundDocs =
        result.root?.children?.map((hit: any) => ({
          docId: hit.fields?.docId as string, // fixed typo: fields, not field
          mailId: hit.fields?.mailId as string,
          updatedAt: hit.fields?.updatedAt as number | undefined,
          userMap: hit.fields?.userMap as Record<string, string>, // undefined if not present
        })) || []

      // Build the result map using original mailIds as keys
      const existenceMap = mailIds.reduce(
        (acc, id) => {
          const cleanedId = id.replace(/<(.*?)>/, "$1")
          const foundDoc = foundDocs.find(
            (doc: { mailId: string }) => doc.mailId === cleanedId,
          )
          acc[id] = {
            docId: foundDoc?.docId ?? "",
            exists: !!foundDoc,
            updatedAt: foundDoc?.updatedAt ?? null,
            userMap: foundDoc?.userMap as Record<string, string>,
          }
          return acc
        },
        {} as Record<
          string,
          {
            docId: string
            exists: boolean
            updatedAt: number | null
            userMap: Record<string, string>
          }
        >,
      )

      return existenceMap
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error checking documents existence:  ${errMessage}`,
        error,
      )
      throw error
    }
  }

  async ifDocumentsExistInSchema(
    schema: VespaSchema,
    docIds: string[],
  ): Promise<Record<string, { exists: boolean; updatedAt: number | null }>> {
    // Construct the YQL query
    const yql = YqlBuilder.create({ requirePermissions: false })
      .select(["docId", "updatedAt"])
      .from(schema)
      .where(inArray("docId", docIds))
      .build()

    const url = `${this.vespaEndpoint}/search/?yql=${encodeURIComponent(yql)}&hits=${docIds.length}`

    try {
      const response = await this.fetchWithRetry(url, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse

      // Extract found documents with their docId and updatedAt
      const foundDocs =
        result.root?.children?.map((hit: any) => ({
          docId: hit.fields.docId as string,
          updatedAt: hit.fields.updatedAt as number | undefined, // undefined if not present
        })) || []

      // Build the result map
      const existenceMap = docIds.reduce(
        (acc, id) => {
          const foundDoc = foundDocs.find(
            (doc: { docId: string }) => doc.docId === id,
          )
          acc[id] = {
            exists: !!foundDoc,
            updatedAt: foundDoc?.updatedAt ?? null, // null if not found or no updatedAt
          }
          return acc
        },
        {} as Record<string, { exists: boolean; updatedAt: number | null }>,
      )

      return existenceMap
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error checking documents existence:  ${errMessage}`,
        error,
      )
      throw error
    }
  }

  async getUsersByNamesAndEmails<T>(payload: T) {
    try {
      const response = await this.fetchWithRetry(
        `${this.vespaEndpoint}/search/`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      )

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to perform user search: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = await response.json()

      // Parse and return the user results
      // const users: VespaUser[] =
      //   data.root.children?.map((child) => {
      //     const fields = child.fields
      //     return VespaUserSchema.parse(fields)
      //   }) || []

      return data
    } catch (error) {
      this.logger.error(`Error searching users: ${error}`, error)
      throw error
    }
  }

  async getItems<T>(payload: T) {
    try {
      const response: Response = await this.fetchWithRetry(
        `${this.vespaEndpoint}/search/`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      )
      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Failed to fetch items: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const data = await response.json()
      return data
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(`Error fetching items: ${errMessage}`, error)
      throw new Error(`Error fetching items: ${errMessage}`)
    }
  }

  async ifMailDocExist(email: string, docId: string): Promise<boolean> {
    // Construct the YQL query using userMap with sameElement
    const yql = YqlBuilder.create({ requirePermissions: false })
      .select("docId")
      .from(mailSchema)
      .where(contains("userMap", sameElement(email, docId)))
      .build()
    const url = `${this.vespaEndpoint}/search/?yql=${encodeURIComponent(yql)}&hits=1&timeout=5s`

    try {
      const response = await this.fetchWithRetry(url, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse

      // Check if document exists
      return !!result.root?.children?.[0]
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error checking documents existence: ${errMessage}`,
        error,
      )
      throw error
    }
  }

  /**
   * Fetches a single random document from a specific schema using the Document V1 API.
   */
  async getRandomDocument(
    namespace: string,
    schema: string,
    cluster: string,
  ): Promise<any | null> {
    // Returning any for now, structure is { documents: [{ id: string, fields: ... }] }
    const url = `${this.vespaEndpoint}/document/v1/${namespace}/${schema}/docid?selection=true&wantedDocumentCount=100&cluster=${cluster}` // Fetch 100 docs
    this.logger.debug(`Fetching 100 random documents from: ${url}`)
    try {
      const response = await this.fetchWithRetry(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      })

      if (!response.ok) {
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(`Vespa error fetching random document: ${errorBody}`)
        throw new Error(
          `Failed to fetch random document: ${response.status} ${response.statusText} - ${errorBody}`,
        )
      }

      const data = (await response.json()) as any
      const docs = data?.documents // Get the array of documents

      // Check if the documents array exists and is not empty
      if (!docs || docs.length === 0) {
        this.logger.warn(
          "Did not find any documents in random sampling response (requested 100)",
          { responseData: data },
        )
        return null
      }

      // Randomly select one document from the list
      const randomIndex = Math.floor(Math.random() * docs.length)
      const selectedDoc = docs[randomIndex]

      this.logger.debug(
        "Randomly selected one document from the fetched list",
        {
          selectedIndex: randomIndex,
          totalDocs: docs.length,
          selectedDocId: selectedDoc?.id,
        },
      )

      return selectedDoc // Return the randomly selected document object { id, fields }
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(`Error fetching random document: ${errMessage}`, error)
      // Rethrow or wrap the error as needed
      throw new Error(`Error fetching random document: ${errMessage}`)
    }
  }

  async getDocumentsBythreadId(
    threadId: string[],
  ): Promise<VespaSearchResponse> {
    const yqlIds = threadId.map((id) => contains("threadId", id))
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(chatMessageSchema)
      .where(or(yqlIds))
      .build()

    const url = `${this.vespaEndpoint}/search/`
    try {
      const payload = {
        yql,
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse
      return result
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(`Error fetching documents with threadId: ${errMessage}`)
    }
  }

  async getEmailsByThreadIds(
    threadIds: string[],
    email: string,
  ): Promise<VespaSearchResponse> {
    const yqlIds = threadIds.map((id) => contains("threadId", id))
    // Include permissions check to ensure user has access to these emails
    const yql = YqlBuilder.create({ email, requirePermissions: true })
      .from(mailSchema)
      .where(or(yqlIds))
      .build()

    const url = `${this.vespaEndpoint}/search/`
    try {
      const payload = {
        yql,
        email: email, // Pass the user's email for permissions check
        hits: 200, // Increased limit to fetch more thread emails
        "ranking.profile": "unranked", // Use unranked for simple retrieval
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        const errorBody = await response.text()
        this.logger.error(
          `getEmailsByThreadIds - Query failed: ${response.status} ${response.statusText} - ${errorBody}`,
        )
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse

      this.logger.info(
        `getEmailsByThreadIds - Results: ${result?.root?.children?.length || 0} emails found for threadIds: ${JSON.stringify(threadIds)}`,
      )

      return result
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `getEmailsByThreadIds - Error: ${errMessage} for threadIds: ${JSON.stringify(threadIds)}`,
      )
      throw new Error(`Error fetching emails by threadIds: ${errMessage}`)
    }
  }

  async getChatUserByEmail(email: string): Promise<VespaSearchResponse> {
    // For user lookup, we typically want to bypass permissions since we're looking up user records directly
    const yql = YqlBuilder.create({
      email: email,
      requirePermissions: false,
    })
      .select()
      .from(chatUserSchema)
      .where(contains("email", email))
      .build()

    const url = `${this.vespaEndpoint}/search/`
    try {
      const payload = {
        yql,
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse
      return result
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(`Error fetching user with email ${email}: ${errMessage}`)
    }
  }

  async getChatContainerIdByChannelName(
    channelName: string,
  ): Promise<VespaSearchResponse> {
    const yql = YqlBuilder.create({ requirePermissions: false })
      .select("docId")
      .from(chatContainerSchema)
      .where(contains("name", channelName))
      .build()

    const url = `${this.vespaEndpoint}/search/`
    try {
      const payload = {
        yql,
      }
      console.log(yql)

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = (await response.json()) as VespaSearchResponse
      return result
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(
        `Error fetching channelId with channel name ${channelName}: ${errMessage}`,
      )
    }
  }

  async getFolderItem(
    docId: string[],
    schema: VespaSchema,
    entity: string,
    email: string,
  ): Promise<VespaSearchResponse> {
    let yqlQuery
    const yqlBuilder = YqlBuilder.create({
      email,
      requirePermissions: true,
    }).from(schema)
    if (!docId.length) {
      // "My Drive" is the special root directory name that Google Drive uses internally
      // while Ingestion we don't get the My Drive Folder , but all its children has parent Name as My Drive
      // to get the items inside My Drive we are using the regex match
      yqlBuilder
        .where(
          or([
            contains("metadata", '{\"parents\":[]}'),
            matches("metadata", "My Drive"),
          ]),
        )
        .limit(400)
    } else {
      const yqlIds = docId.map((id) => contains("parentId", id))
      yqlBuilder.where(or(yqlIds)).limit(400)
    }
    const url = `${this.vespaEndpoint}/search/`
    try {
      const payload = {
        yql: yqlBuilder.build(),
      }

      const response = await this.fetchWithRetry(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorText = response.statusText
        throw new Error(
          `Search query failed: ${response.status} ${response.statusText} - ${errorText}`,
        )
      }

      const result = await response.json()
      return result as VespaSearchResponse
    } catch (error) {
      const errMessage = getErrorMessage(error)
      throw new Error(
        `Error fetching folderItem with folderId ${docId.join(",")}: ${errMessage}`,
      )
    }
  }
}

export default VespaClient
