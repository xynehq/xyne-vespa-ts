import {
  Apps,
  CalendarEntity,
  DriveEntity,
  eventSchema,
  MailEntity,
  fileSchema,
  mailSchema,
  userQuerySchema,
  userSchema,
  mailAttachmentSchema,
  chatUserSchema,
  chatMessageSchema,
  datasourceSchema,
  dataSourceFileSchema,
  type VespaDataSource,
  type VespaDataSourceFile,
  type VespaDataSourceSearch,
  type Intent,
  type Span,
  SlackEntity,
  chatContainerSchema,
  KbItemsSchema,
  type CollectionVespaIds,
} from "./types"
import type {
  VespaAutocompleteResponse,
  VespaFile,
  VespaMail,
  VespaSearchResult,
  VespaSearchResponse,
  VespaUser,
  VespaGetResult,
  Entity,
  VespaEvent,
  VespaUserQueryHistory,
  VespaSchema,
  VespaMailAttachment,
  VespaChatContainer,
  Inserts,
  VespaChatUserSearchSchema,
  VespaSearchResults,
  ChatUserCore,
  VespaQueryConfig,
  GetItemsParams,
  GetThreadItemsParams,
} from "./types"
import { SearchModes } from "./types"
import {
  dateToUnixTimestamp,
  escapeYqlValue,
  getErrorMessage,
  processGmailIntent,
} from "./utils"
import {
  ErrorDeletingDocuments,
  ErrorRetrievingDocuments,
  ErrorPerformingSearch,
  ErrorInsertingDocument,
} from "./errors"
import crypto from "crypto"
import VespaClient from "./client/vespaClient"
import pLimit from "p-limit"
import type { ILogger, VespaConfig, VespaDependencies } from "./types"
import { is } from "zod/locales"

type YqlProfile = {
  profile: SearchModes
  yql: string
}

interface EntityCounts {
  [entity: string]: number
}

export interface AppEntityCounts {
  [app: string]: EntityCounts
}

const AllSources = [
  fileSchema,
  userSchema,
  mailSchema,
  eventSchema,
  mailAttachmentSchema,
  chatUserSchema,
  chatMessageSchema,
  chatContainerSchema,
  datasourceSchema,
  dataSourceFileSchema,
  KbItemsSchema,
]

export class VespaService {
  private logger: ILogger
  private config: VespaConfig
  private vespa: VespaClient
  private schemaSources: string[]
  private vespaEndpoint: string
  constructor(dependencies: VespaDependencies) {
    this.logger = dependencies.logger.child({ module: "vespa" })
    this.config = dependencies.config
    this.schemaSources = dependencies.sourceSchemas || AllSources
    this.vespaEndpoint = dependencies.vespaEndpoint
    // Initialize Vespa clients
    this.vespa = new VespaClient(this.vespaEndpoint, this.logger, this.config)
  }

  getSchemaSources(): string {
    return this.schemaSources.join(", ")
  }
  /**
   * Deletes all documents from the specified schema and namespace in Vespa.
   */
  async deleteAllDocuments() {
    return this.vespa
      .deleteAllDocuments({
        cluster: this.config.cluster,
        namespace: this.config.namespace,
        schema: fileSchema,
      })
      .catch((error) => {
        this.logger.error(`Deleting documents failed with error:`, error)
        throw new ErrorDeletingDocuments({
          cause: error as Error,
          sources: this.getSchemaSources(),
        })
      })
  }

  insertDocument = async (document: VespaFile) => {
    return this.vespa
      .insertDocument(document, {
        namespace: this.config.namespace,
        schema: fileSchema,
      })
      .catch((error) => {
        this.logger.error(`Inserting document failed with error:`, error)
        throw new ErrorInsertingDocument({
          docId: document.docId,
          cause: error as Error,
          sources: fileSchema,
        })
      })
  }

  // Renamed to reflect its purpose: retrying a single insert
  insertWithRetry = async (
    document: Inserts,
    schema: VespaSchema,
    maxRetries = 8,
  ) => {
    let lastError: any
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        await this.vespa.insert(document, {
          namespace: this.config.namespace,
          schema,
        })
        this.logger.debug(`Inserted document ${document.docId}`)
        return
      } catch (error) {
        lastError = error
        if (
          (error as Error).message.includes("429 Too Many Requests") &&
          attempt < maxRetries
        ) {
          const delayMs = Math.pow(2, attempt) * 2000
          this.logger.warn(
            `Vespa 429 for ${document.docId}, retrying in ${delayMs}ms (attempt ${attempt + 1})`,
          )
          await new Promise((resolve) => setTimeout(resolve, delayMs))
        } else {
          throw new Error(
            `Error inserting document ${document.docId}: ${(error as Error).message}`,
          )
        }
      }
    }
    throw new Error(
      `Failed to insert ${document.docId} after ${maxRetries} retries: ${lastError.message}`,
    )
  }

  // generic insert method
  insert = async (document: Inserts, schema: VespaSchema) => {
    return this.vespa
      .insert(document, { namespace: this.config.namespace, schema })
      .catch((error) => {
        this.logger.error(`Inserting document failed with error:`, error)
        throw new ErrorInsertingDocument({
          docId: document.docId,
          cause: error as Error,
          sources: schema,
        })
      })
  }

  insertUser = async (user: VespaUser) => {
    return this.vespa
      .insertUser(user, {
        namespace: this.config.namespace,
        schema: userSchema,
      })
      .catch((error) => {
        this.logger.error(`Inserting user failed with error:`, error)
        throw new ErrorInsertingDocument({
          docId: user.docId,
          cause: error as Error,
          sources: userSchema,
        })
      })
  }

  deduplicateAutocomplete = (
    resp: VespaAutocompleteResponse,
  ): VespaAutocompleteResponse => {
    const { root } = resp
    if (!root.children) {
      return resp
    }
    const uniqueResults = []
    const emails = new Set()
    for (const child of root.children) {
      // @ts-ignore
      const email = child.fields.email
      if (email && !emails.has(email)) {
        emails.add(email)
        uniqueResults.push(child)
      } else if (!email) {
        uniqueResults.push(child)
      }
    }
    resp.root.children = uniqueResults
    return resp
  }

  autocomplete = async (
    query: string,
    email: string,
    limit: number = 5,
  ): Promise<VespaAutocompleteResponse> => {
    const sources = this.getSchemaSources()
      .split(", ")
      .filter((s) => s !== chatMessageSchema)
      .join(", ")

    // Construct the YQL query for fuzzy prefix matching with maxEditDistance:2
    // the drawback here is that for user field we will get duplicates, for the same
    // email one contact and one from user directory
    const yqlQuery = `select * from sources ${sources}, ${userQuerySchema}
    where
        (title_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
        and permissions contains @email)
        or
        (
            (name_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
            and owner contains @email)
            or
            (email_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
            and owner contains @email)
        )
        or
        (
            (name_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
            and app contains "${Apps.GoogleWorkspace}")
            or
            (email_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
            and app contains "${Apps.GoogleWorkspace}")
        )
        or
        (subject_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
        and permissions contains @email)
        or
        (name_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
        and permissions contains @email)
        or
        (query_text contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
        and owner contains @email)
        or
        (
          (
            name_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query)) or
            email_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))
          )
          and permissions contains @email
        )
        `

    const searchPayload = {
      yql: yqlQuery,
      query,
      email: email,
      hits: limit, // Limit the number of suggestions
      "ranking.profile": "autocomplete", // Use the autocomplete rank profile
      "presentation.summary": "autocomplete",
      timeout: "5s",
    }

    return this.vespa.autoComplete(searchPayload).catch((error) => {
      this.logger.error(`Autocomplete failed with error:`, error)
      throw new ErrorPerformingSearch({
        message: `Error performing autocomplete search`,
        cause: error as Error,
        sources: "file",
      })
      // TODO: instead of null just send empty response
      throw error
    })
  }

  handleAppsNotInYql = (app: Apps | null, includedApp: Apps[]) => {
    this.logger.error(`${app} is not supported in YQL queries yet`)
    throw new ErrorPerformingSearch({
      message: `${app} is not supported in YQL queries yet`,
      sources: includedApp.join(", "),
    })
  }

  // TODO: it seems the owner part is complicating things
  HybridDefaultProfile = (
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    profile: SearchModes = SearchModes.NativeRank,
    timestampRange?: { to: number | null; from: number | null } | null,
    excludedIds?: string[],
    notInMailLabels?: string[],
    excludedApps?: Apps[],
    intent?: Intent | null,
  ): YqlProfile => {
    // Helper function to build timestamp conditions
    const buildTimestampConditions = (fromField: string, toField: string) => {
      const conditions: string[] = []
      if (timestampRange?.from) {
        conditions.push(`${fromField} >= ${timestampRange.from}`)
      }
      if (timestampRange?.to) {
        conditions.push(`${toField} <= ${timestampRange.to}`)
      }
      return conditions.join(" and ")
    }

    // ToDo we have to handle this filter as we are applying multiple times app filtering
    // Helper function to build app/entity filter
    const buildAppEntityFilter = () => {
      return `${
        app
          ? (Array.isArray(app) && app.length > 0)
            ? `and (${app.map((a) => `app contains '${escapeYqlValue(a)}'`).join(" or ")})`
            : "and app contains @app"
          : ""
      } ${
        entity
          ? Array.isArray(entity) && entity.length > 0
            ? `and (${entity.map((e) => `entity contains '${escapeYqlValue(e)}'`).join(" or ")})`
            : "and entity contains @entity"
          : ""
      }`.trim()
    }

    // Helper function to build exclusion condition
    const buildExclusionCondition = () => {
      if (!excludedIds || excludedIds.length === 0) return ""
      return excludedIds.map((id) => `docId contains '${id}'`).join(" or ")
    }

    // Helper function to build mail label query
    const buildMailLabelQuery = () => {
      if (!notInMailLabels || notInMailLabels.length === 0) return ""
      return `and !(${notInMailLabels.map((label) => `labels contains '${label}'`).join(" or ")})`
    }
    const intentFilter = intent ? this.buildIntentFilter(intent) : ""

    // App-specific YQL builders
    const buildGoogleWorkspaceYQL = () => {
      const userTimestamp = buildTimestampConditions(
        "creationTime",
        "creationTime",
      )
      const appOrEntityFilter = buildAppEntityFilter()
      const hasAppOrEntity = !!(app || entity)
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${userTimestamp.length ? `and (${userTimestamp})` : ""}
        ${
          !hasAppOrEntity
            ? `and app contains "${Apps.GoogleWorkspace}"`
            : `${appOrEntityFilter} and permissions contains @email`
        }
        ${intentFilter}
      )
      or
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        and owner contains @email
        ${userTimestamp.length ? `and ${userTimestamp}` : ""}
        ${appOrEntityFilter}
      )`
    }

    const buildGmailYQL = () => {
      const mailTimestamp = buildTimestampConditions("timestamp", "timestamp")
      const appOrEntityFilter = buildAppEntityFilter()
      const mailLabelQuery = buildMailLabelQuery()
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${mailTimestamp.length ? `and (${mailTimestamp})` : ""}
        and permissions contains @email
        ${mailLabelQuery}
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }

    const buildDefaultYQL = () => {
      const appOrEntityFilter = buildAppEntityFilter()
      const timestamp = buildTimestampConditions("updatedAt", "updatedAt")
      return ` 
  (
      (
            ({targetHits:${hits}} userInput(@query))
            or
            ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
      )
      and (permissions contains @email or owner contains @email)
      ${timestamp.length ? `and (${timestamp})` : ""}
      ${appOrEntityFilter}
      ${intentFilter}
  )
  `
    }

    const buildGoogleDriveYQL = () => {
      const fileTimestamp = buildTimestampConditions("updatedAt", "updatedAt")
      const appOrEntityFilter = buildAppEntityFilter()
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${fileTimestamp.length ? `and (${fileTimestamp})` : ""}
        and permissions contains @email
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }

    const buildGoogleCalendarYQL = () => {
      const eventTimestamp = buildTimestampConditions("startTime", "startTime")
      const appOrEntityFilter = buildAppEntityFilter()
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${eventTimestamp.length ? `and (${eventTimestamp})` : ""}
        and permissions contains @email
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }

    const buildSlackYQL = () => {
      const appOrEntityFilter = buildAppEntityFilter()
      const timestamp = buildTimestampConditions("updatedAt", "updatedAt")
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(text_embeddings, e))
        )
        ${timestamp.length ? `and (${timestamp})` : ""}
        ${appOrEntityFilter}
        and permissions contains @email
      )`
    }

    // Start with AllSources and filter out excluded app schemas
    let newSources = this.getSchemaSources()
    if (excludedApps && excludedApps.length > 0) {
      let sourcesToExclude: string[] = []

      excludedApps.forEach((excludedApp) => {
        switch (excludedApp) {
          case Apps.Slack:
            sourcesToExclude.push(chatMessageSchema, chatUserSchema)
            break
          case Apps.Gmail:
            sourcesToExclude.push(mailSchema, mailAttachmentSchema)
            break
          case Apps.GoogleDrive:
            sourcesToExclude.push(fileSchema)
            break
          case Apps.GoogleCalendar:
            sourcesToExclude.push(eventSchema)
            break
          case Apps.GoogleWorkspace:
            sourcesToExclude.push(userSchema)
            break
        }
      })
      newSources = newSources
        .split(", ")
        .filter((source) => !sourcesToExclude.includes(source))
        .join(", ")
    }

    // Start with all apps and filter out excluded ones
    const allApps = Object.values(Apps)
    const includedApps = allApps.filter(
      (appItem) => !excludedApps?.includes(appItem),
    )

    // Build app-specific queries for included apps
    const appQueries: string[] = []

    for (const includedApp of includedApps) {
      switch (includedApp) {
        case Apps.GoogleWorkspace:
          appQueries.push(buildGoogleWorkspaceYQL())
          break
        case Apps.Gmail:
          appQueries.push(buildGmailYQL())
          break
        case Apps.GoogleDrive:
          appQueries.push(buildGoogleDriveYQL())
          break
        case Apps.GoogleCalendar:
          appQueries.push(buildGoogleCalendarYQL())
          break
        case Apps.Slack:
          appQueries.push(buildSlackYQL())
          break
        case Apps.DataSource:
          break
        default:
          appQueries.push(buildDefaultYQL())
          break
      }
    }

    // Combine all queries
    const combinedQuery = appQueries.join("\nor\n")
    const exclusionCondition = buildExclusionCondition()

    return {
      profile: profile,
      yql: `
    select * from sources ${newSources}
        where (
          (
            ${combinedQuery}
          )
          ${exclusionCondition ? `and !(${exclusionCondition})` : ""}
        )
    `,
    }
  }
  // Helper function to build intent filter
  buildIntentFilter = (intent: Intent | null) => {
    const intentFilters: string[] = []
    if (intent?.from && intent.from.length > 0) {
      intentFilters.push(
        intent.from.map((from) => `\"from\" contains '${from}'`).join(" or "),
      )
    }
    if (intent?.to && intent.to.length > 0) {
      intentFilters.push(
        intent.to.map((to) => `to contains '${to}'`).join(" or "),
      )
    }
    if (intent?.cc && intent.cc.length > 0) {
      intentFilters.push(
        intent.cc.map((cc) => `cc contains '${cc}'`).join(" or "),
      )
    }
    if (intent?.bcc && intent.bcc.length > 0) {
      intentFilters.push(
        intent.bcc.map((bcc) => `bcc contains '${bcc}'`).join(" or "),
      )
    }
    return intentFilters.length > 0
      ? "and" + " " + intentFilters.join(" and ")
      : ""
  }

  HybridDefaultProfileForAgent = (
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    profile: SearchModes = SearchModes.NativeRank,
    timestampRange?: { to: number | null; from: number | null } | null,
    excludedIds?: string[],
    notInMailLabels?: string[],
    AllowedApps: Apps[] | null = null,
    dataSourceIds: string[] = [],
    intent: Intent | null = null,
    channelIds: string[] = [],
    processedCollectionSelections: CollectionVespaIds = {},
    driveIds: string[] = [],
    selectedItem: {} = {},
  ): YqlProfile => {
    // Helper function to build timestamp conditions
    const buildTimestampConditions = (fromField: string, toField: string) => {
      const conditions: string[] = []
      if (timestampRange?.from) {
        conditions.push(`${fromField} >= ${timestampRange.from}`)
      }
      if (timestampRange?.to) {
        conditions.push(`${toField} <= ${timestampRange.to}`)
      }
      return conditions.join(" and ")
    }

    // helper function to build docId inclusion condition
    const buildDocsInclusionCondition = (fieldName: string, ids: string[]) => {
      if (!ids || ids.length === 0) return ""

      const conditions = ids.map((id) => `${fieldName} contains '${id.trim()}'`)
      return conditions.join(" or ")
    }

    // Helper function to build app/entity filter
    const buildAppEntityFilter = () => {
      return `${
        app
          ? (Array.isArray(app) && app.length > 0)
            ? `and (${app.map((a) => `app contains '${escapeYqlValue(a)}'`).join(" or ")})`
            : "and app contains @app"
          : ""
      } ${
        entity
          ? Array.isArray(entity) && entity.length > 0
            ? `and (${entity.map((e) => `entity contains '${escapeYqlValue(e)}'`).join(" or ")})`
            : "and entity contains @entity"
          : ""
      }`.trim()
    }
    // Helper function to build exclusion condition
    const buildExclusionCondition = () => {
      if (!excludedIds || excludedIds.length === 0) return ""
      return excludedIds.map((id) => `docId contains '${id}'`).join(" or ")
    }
    // Helper function to build mail label query
    const buildMailLabelQuery = () => {
      if (!notInMailLabels || notInMailLabels.length === 0) return ""
      return `and !(${notInMailLabels.map((label) => `labels contains '${label}'`).join(" or ")})`
    }
    // App-specific YQL builders
    const buildGoogleWorkspaceYQL = () => {
      const userTimestamp = buildTimestampConditions(
        "creationTime",
        "creationTime",
      )
      const appOrEntityFilter = buildAppEntityFilter()
      const hasAppOrEntity = !!(app || entity)
      const intentFilter = this.buildIntentFilter(intent)
      return `
      (
        ({targetHits:${hits}} userInput(@query))
        ${timestampRange ? `and (${userTimestamp})` : ""}
        ${
          !hasAppOrEntity
            ? `and app contains "${Apps.GoogleWorkspace}"`
            : `${appOrEntityFilter} and permissions contains @email`
        }
      )
      or
      (
        ({targetHits:${hits}} userInput(@query))
        and owner contains @email
        ${timestampRange ? `and ${userTimestamp}` : ""}
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }
    const buildGmailYQL = () => {
      const mailTimestamp = buildTimestampConditions("timestamp", "timestamp")
      const appOrEntityFilter = buildAppEntityFilter()
      const mailLabelQuery = buildMailLabelQuery()
      const intentFilter = this.buildIntentFilter(intent)
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${timestampRange ? `and (${mailTimestamp})` : ""}
        and permissions contains @email
        ${mailLabelQuery}
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }
    const buildGoogleDriveYQL = () => {
      const fileTimestamp = buildTimestampConditions("updatedAt", "updatedAt")
      const appOrEntityFilter = buildAppEntityFilter()
      // let driveItem:string [] = []
      // if we have some DriveIds then we are going to fetch all the items in that folder
      const intentFilter = this.buildIntentFilter(intent)
      let driveItem: string[] = []
      if ((selectedItem as any)[Apps.GoogleDrive]) {
        driveItem = [...(selectedItem as any)[Apps.GoogleDrive]]
      }

      const driveIdConditions = buildDocsInclusionCondition("docId", driveIds)
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${timestampRange ? `and (${fileTimestamp})` : ""}
        and permissions contains @email
        ${appOrEntityFilter}
        ${driveIdConditions ? `and ${driveIdConditions}` : ""}
        ${intentFilter}
      )
     `
    }
    const buildGoogleCalendarYQL = () => {
      const eventTimestamp = buildTimestampConditions("startTime", "startTime")
      const appOrEntityFilter = buildAppEntityFilter()
      const intentFilter = this.buildIntentFilter(intent)
      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(chunk_embeddings, e))
        )
        ${timestampRange ? `and (${eventTimestamp})` : ""}
        and permissions contains @email
        ${appOrEntityFilter}
        ${intentFilter}
      )`
    }
    const buildSlackYQL = () => {
      const appOrEntityFilter = buildAppEntityFilter()
      let channelIds: string[] = []
      const intentFilter = this.buildIntentFilter(intent)
      channelIds = (selectedItem as Record<string, unknown>)[Apps.Slack] as any
      const channelIdConditions = buildDocsInclusionCondition(
        "channelId",
        channelIds,
      )

      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(text_embeddings, e))
        )
        ${appOrEntityFilter}
        and permissions contains @email
        ${channelIdConditions ? `and ${channelIdConditions}` : ""}
      )`
    }

    const buildDataSourceFileYQL = () => {
      // For DataSourceFile, app and entity might not be directly applicable in the same way,
      // but keeping appOrEntityFilter for consistency if needed for other metadata.

      const dsIds = (selectedItem as Record<string, unknown>)[
        Apps.DataSource
      ] as any
      const dataSourceIdConditions = buildDocsInclusionCondition(
        "dataSourceId",
        dsIds,
      )

      return `
      (
        (
          ({targetHits:${hits}}userInput(@query))
          or
          ({targetHits:${hits}}nearestNeighbor(chunk_embeddings, e))
        ) 
        and ${dataSourceIdConditions}
      )`
    }

    const buildCollectionConditions = (
      processedSelections: CollectionVespaIds,
    ) => {
      let conditions: string[] = []

      if (
        processedSelections.collectionIds &&
        processedSelections.collectionIds.length > 0
      ) {
        const collectionCondition = `(${processedSelections.collectionIds.map((id: string) => `clId contains '${id.trim()}'`).join(" or ")})`
        conditions.push(collectionCondition)
      }

      if (
        processedSelections.collectionFolderIds &&
        processedSelections.collectionFolderIds.length > 0
      ) {
        const folderCondition = `(${processedSelections.collectionFolderIds.map((id: string) => `clFd contains '${id.trim()}'`).join(" or ")})`
        conditions.push(folderCondition)
      }

      if (
        processedSelections.collectionFileIds &&
        processedSelections.collectionFileIds.length > 0
      ) {
        const fileCondition = `(${processedSelections.collectionFileIds.map((id: string) => `docId contains '${id.trim()}'`).join(" or ")})`
        conditions.push(fileCondition)
      }
      return conditions
    }

    const buildCollectionFileYQL = (conditions: string[]) => {
      const finalCondition = `(${conditions.join(" or ")})`
      return `
      (
        (
          ({targetHits:${hits}}userInput(@query))
          or
          ({targetHits:${hits}}nearestNeighbor(chunk_embeddings, e))
        ) 
        and ${finalCondition}
      )`
    }

    // Build app-specific queries and sources
    const appQueries: string[] = []
    const sources: string[] = []

    if (AllowedApps && AllowedApps.length > 0) {
      for (const allowedApp of AllowedApps) {
        switch (allowedApp) {
          case Apps.GoogleWorkspace:
            appQueries.push(buildGoogleWorkspaceYQL())
            if (!sources.includes(userSchema)) sources.push(userSchema)
            break
          case Apps.Gmail:
            appQueries.push(buildGmailYQL())
            if (!sources.includes(mailSchema)) sources.push(mailSchema)
            break
          case Apps.GoogleDrive:
            const googleDriveYQL = buildGoogleDriveYQL()
            appQueries.push(googleDriveYQL)
            if (!sources.includes(fileSchema)) sources.push(fileSchema)
            break
          case Apps.GoogleCalendar:
            appQueries.push(buildGoogleCalendarYQL())
            if (!sources.includes(eventSchema)) sources.push(eventSchema)
            break
          case Apps.Slack:
            appQueries.push(buildSlackYQL())
            if (!sources.includes(chatUserSchema)) sources.push(chatUserSchema)
            if (!sources.includes(chatMessageSchema))
              sources.push(chatMessageSchema)
            if (!sources.includes(chatContainerSchema))
              sources.push(chatContainerSchema)
            break
          case Apps.DataSource:
            appQueries.push(buildDataSourceFileYQL())
            if (!sources.includes(dataSourceFileSchema))
              sources.push(dataSourceFileSchema)
            break
          case Apps.KnowledgeBase:
            const collectionConditions = buildCollectionConditions(
              processedCollectionSelections,
            )
            if (collectionConditions.length > 0) {
              const collectionQuery =
                buildCollectionFileYQL(collectionConditions)
              appQueries.push(collectionQuery)
              if (!sources.includes(KbItemsSchema)) sources.push(KbItemsSchema)
            } else {
              this.logger.warn(
                "Apps.KnowledgeBase specified for agent, but no valid processedCollectionSelections found. Skipping KnowledgeBase search part.",
              )
            }
            break
        }
      }
    } else if (dataSourceIds && dataSourceIds.length > 0) {
      // This handles the case where AllowedApps might be empty or null,
      // but specific dataSourceIds are provided (e.g., agent is only for specific data sources).
      appQueries.push(buildDataSourceFileYQL())
      if (!sources.includes(dataSourceFileSchema))
        sources.push(dataSourceFileSchema)
    }
    if (channelIds.length > 0) {
      appQueries.push(buildSlackYQL())
      if (!sources.includes(chatUserSchema)) sources.push(chatUserSchema)
      if (!sources.includes(chatMessageSchema)) sources.push(chatMessageSchema)
      if (!sources.includes(chatContainerSchema))
        sources.push(chatContainerSchema)
    }

    // Combine all queries
    const combinedQuery = appQueries.join("\n    or\n    ")
    const exclusionCondition = buildExclusionCondition()
    const sourcesString = [...new Set(sources)].join(", ") // Ensure unique sources

    // If sourcesString is empty (e.g., only Apps.DataSource was specified but no dataSourceIds were provided,
    // or no valid AllowedApps were given), then the YQL query will be invalid.
    const fromClause = sourcesString ? `from sources ${sourcesString}` : ""

    const finalYql = `
    select *
    ${fromClause} 
    where
    (
      (
        ${combinedQuery}
      )
      ${exclusionCondition ? `and !(${exclusionCondition})` : ""}
    )
    ;
    `

    return {
      profile: profile,
      yql: finalYql,
    }
  }

  HybridDefaultProfileInFiles = (
    hits: number,
    profile: SearchModes = SearchModes.NativeRank,
    fileIds: string[],
    notInMailLabels?: string[],
  ): YqlProfile => {
    let mailLabelQuery = ""
    if (notInMailLabels && notInMailLabels.length > 0) {
      mailLabelQuery = `and !(${notInMailLabels.map((label) => `labels contains '${label}'`).join(" or ")})`
    }

    const contextClauses: string[] = []

    if (fileIds?.length) {
      const idFilters = fileIds.map((id) => `docId contains '${id}'`)
      contextClauses.push(...idFilters)
    }

    const specificContextQuery = contextClauses.length
      ? `and (${contextClauses.join(" or ")})`
      : ""

    // the last 2 'or' conditions are due to the 2 types of users, contacts and admin directory present in the same schema
    return {
      profile: profile,
      yql: `
        select * from sources ${AllSources}
        where ((
          (
            (
              ({targetHits:${hits}}userInput(@query))
              or
              ({targetHits:${hits}}nearestNeighbor(chunk_embeddings, e))
            )
            and permissions contains @email ${mailLabelQuery}
            ${specificContextQuery} 
          )
            or
            (
              (
              ({targetHits:${hits}}userInput(@query))
              or
              ({targetHits:${hits}}nearestNeighbor(text_embeddings, e))
            )
              and permissions contains @email ${specificContextQuery}
            )
          or
          (
            ({targetHits:${hits}}userInput(@query))
            and permissions contains @email ${specificContextQuery}
          )
          or
          (
            ({targetHits:${hits}}userInput(@query))
            and owner contains @email
            ${specificContextQuery}
          )
        )
      )`,
    }
  }

  HybridDefaultProfileForSlack = (
    hits: number,
    profile: SearchModes = SearchModes.NativeRank,
    channelIds?: string[],
    threadId?: string,
    userId?: string,
    timestampRange?: { to: number | null; from: number | null } | null,
  ): YqlProfile => {
    // Helper function to build timestamp conditions
    const buildTimestampConditions = (fromField: string, toField: string) => {
      const conditions: string[] = []
      if (timestampRange?.from) {
        conditions.push(`${fromField} >= ${timestampRange.from}`)
      }
      if (timestampRange?.to) {
        conditions.push(`${toField} <= ${timestampRange.to}`)
      }
      return conditions.join(" and ")
    }

    const timestampCondition = buildTimestampConditions(
      "createdAt",
      "createdAt",
    )

    const channelIdConditions =
      channelIds && channelIds.length > 0
        ? `(${channelIds.map((id) => `channelId contains '${id}'`).join(" or ")})`
        : ""

    const threadIdCondition = threadId ? `threadId contains '${threadId}'` : ""
    const userIdCondition = userId ? `userId contains '${userId}'` : ""

    const conditions = [
      timestampCondition,
      channelIdConditions,
      threadIdCondition,
      userIdCondition,
    ]
      .filter(Boolean)
      .join(" and ")

    const yql = `
    select * from sources ${chatMessageSchema}
    where (
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(text_embeddings, e))
        )
        and permissions contains @email
        ${conditions ? `and ${conditions}` : ""}
      )
    )
  `

    return {
      profile: profile,
      yql: yql,
    }
  }

  HybridDefaultProfileAppEntityCounts = (
    hits: number,
    timestampRange: { to: number; from: number } | null,
    notInMailLabels?: string[],
    excludedApps?: Apps[],
  ): YqlProfile => {
    // Helper function to build timestamp conditions
    const buildTimestampConditions = (fromField: string, toField: string) => {
      const conditions: string[] = []
      if (timestampRange?.from) {
        conditions.push(`${fromField} >= ${timestampRange.from}`)
      }
      if (timestampRange?.to) {
        conditions.push(`${toField} <= ${timestampRange.to}`)
      }
      return conditions.join(" and ")
    }

    // Helper function to build mail label query
    const buildMailLabelQuery = () => {
      if (!notInMailLabels || notInMailLabels.length === 0) return ""
      return `and !(${notInMailLabels.map((label) => `labels contains '${label}'`).join(" or ")})`
    }

    // Validate timestamp range
    if (timestampRange && !timestampRange.from && !timestampRange.to) {
      throw new Error("Invalid timestamp range")
    }

    // App-specific YQL builders for counting
    const buildFilesAndMailYQL = () => {
      const fileTimestamp = buildTimestampConditions("updatedAt", "updatedAt")
      const mailTimestamp = buildTimestampConditions("timestamp", "timestamp")
      const mailLabelQuery = buildMailLabelQuery()

      return `
      (
        (
          ({targetHits:${hits}}userInput(@query))
          or
          ({targetHits:${hits}}nearestNeighbor(chunk_embeddings, e))
        )
        ${timestampRange ? `and (${fileTimestamp} or ${mailTimestamp})` : ""}
        and permissions contains @email
        ${mailLabelQuery}
      )`
    }

    const buildTextEmbeddingsYQL = () => {
      const fileTimestamp = buildTimestampConditions("updatedAt", "updatedAt")
      const mailTimestamp = buildTimestampConditions("timestamp", "timestamp")

      return `
      (
        (
          ({targetHits:${hits}}userInput(@query))
          or
          ({targetHits:${hits}}nearestNeighbor(text_embeddings, e))
        )
        ${timestampRange ? `and (${fileTimestamp} or ${mailTimestamp})` : ""}
        and permissions contains @email
      )`
    }

    const buildGoogleWorkspaceYQL = () => {
      const userTimestamp = buildTimestampConditions(
        "creationTime",
        "creationTime",
      )

      return `
      (
        ({targetHits:${hits}}userInput(@query))
        ${timestampRange ? `and ${userTimestamp}` : ""}
        and app contains "${Apps.GoogleWorkspace}"
      )`
    }

    const buildOwnerYQL = () => {
      const userTimestamp = buildTimestampConditions(
        "creationTime",
        "creationTime",
      )

      return `
      (
        ({targetHits:${hits}}userInput(@query))
        and owner contains @email
        ${timestampRange ? `and ${userTimestamp}` : ""}
      )`
    }

    // Start with AllSources and filter out excluded app schemas
    let newSources = this.getSchemaSources()
    if (excludedApps && excludedApps.length > 0) {
      let sourcesToExclude: string[] = []

      excludedApps.forEach((excludedApp) => {
        switch (excludedApp) {
          case Apps.Slack:
            sourcesToExclude.push(chatMessageSchema, chatUserSchema)
            break
          case Apps.Gmail:
            sourcesToExclude.push(mailSchema, mailAttachmentSchema)
            break
          case Apps.GoogleDrive:
            sourcesToExclude.push(fileSchema)
            break
          case Apps.GoogleCalendar:
            sourcesToExclude.push(eventSchema)
            break
          case Apps.GoogleWorkspace:
            sourcesToExclude.push(userSchema)
            break
        }
      })

      // Filter out excluded schemas from AllSources
      newSources = this.getSchemaSources()
        .split(", ")
        .filter((source) => !sourcesToExclude.includes(source))
        .join(", ")
    }

    // Build the combined query using modular components
    const filesAndMailQuery = buildFilesAndMailYQL()
    const textEmbeddingsQuery = buildTextEmbeddingsYQL()
    const googleWorkspaceQuery = buildGoogleWorkspaceYQL()
    const ownerQuery = buildOwnerYQL()

    return {
      profile: SearchModes.NativeRank,
      yql: `select * from sources ${newSources}
            where (
              ${filesAndMailQuery}
              or
              ${textEmbeddingsQuery}
              or
              ${googleWorkspaceQuery}
              or
              ${ownerQuery}
            )
            limit 0
            | all(
                group(app) each(
                    group(entity) each(output(count()))
                )
            )`,
    }
  }

  getAllDocumentsForAgent = async (
    AllowedApps: Apps[] | null,
    dataSourceIds: string[] = [],
    limit: number = 400,
  ): Promise<VespaSearchResponse | null> => {
    const sources: string[] = []
    const conditions: string[] = []

    if (AllowedApps && AllowedApps.length > 0) {
      for (const allowedApp of AllowedApps) {
        switch (allowedApp) {
          case Apps.GoogleWorkspace:
            if (!sources.includes(userSchema)) sources.push(userSchema)
            conditions.push(`app contains "${Apps.GoogleWorkspace}"`)
            break
          case Apps.Gmail:
            if (!sources.includes(mailSchema)) sources.push(mailSchema)
            conditions.push(`app contains "${Apps.Gmail}"`)
            break
          case Apps.GoogleDrive:
            if (!sources.includes(fileSchema)) sources.push(fileSchema)
            conditions.push(`app contains "${Apps.GoogleDrive}"`)
            break
          case Apps.GoogleCalendar:
            if (!sources.includes(eventSchema)) sources.push(eventSchema)
            conditions.push(`app contains "${Apps.GoogleCalendar}"`)
            break
          case Apps.Slack:
            if (!sources.includes(chatUserSchema)) sources.push(chatUserSchema)
            if (!sources.includes(chatMessageSchema))
              sources.push(chatMessageSchema)
            conditions.push(`app contains "${Apps.Slack}"`)
            break
          case Apps.DataSource:
            if (dataSourceIds && dataSourceIds.length > 0) {
              if (!sources.includes(dataSourceFileSchema))
                sources.push(dataSourceFileSchema)
              const dsConditions = dataSourceIds
                .map((id) => `dataSourceId contains '${id.trim()}'`)
                .join(" or ")
              conditions.push(`(${dsConditions})`)
            }
            break
        }
      }
    } else if (dataSourceIds && dataSourceIds.length > 0) {
      if (!sources.includes(dataSourceFileSchema))
        sources.push(dataSourceFileSchema)
      const dsConditions = dataSourceIds
        .map((id) => `dataSourceId contains '${id.trim()}'`)
        .join(" or ")
      conditions.push(`(${dsConditions})`)
    }
    //return null
    const sourcesString = [...new Set(sources)].join(", ")
    if (!sourcesString) {
      return null
    }

    const whereClause = `where ${conditions.join(" or ")}`
    const yql = `select * from sources ${sourcesString} ${whereClause}`

    const payload = {
      yql,
      hits: limit,
      timeout: "30s",
      "ranking.profile": "unranked",
    }

    return this.vespa.search<VespaSearchResponse>(payload).catch((error) => {
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: sourcesString,
      })
    })
  }

  groupVespaSearch = async (
    query: string,
    email: string,
    limit = this.config.page,
    isSlackConnected: boolean,
    isGmailConnected: boolean,
    isCalendarConnected: boolean,
    isDriveConnected: boolean,
    timestampRange?: { to: number; from: number } | null,
  ): Promise<AppEntityCounts> => {
    return await this._groupVespaSearch(
      query,
      email,
      limit,
      timestampRange,
      isSlackConnected,
      isGmailConnected,
      isCalendarConnected,
      isDriveConnected,
    )
  }
  async _groupVespaSearch(
    query: string,
    email: string,
    limit = this.config.page,
    timestampRange?: { to: number; from: number } | null,
    isSlackConnected?: boolean,
    isGmailConnected?: boolean,
    isCalendarConnected?: boolean,
    isDriveConnected?: boolean,
  ): Promise<AppEntityCounts> {
    let excludedApps: Apps[] = []
    try {
      if (!isDriveConnected) {
        excludedApps.push(Apps.GoogleDrive)
      }
      if (!isCalendarConnected) {
        excludedApps.push(Apps.GoogleCalendar)
      }
      if (!isGmailConnected) {
        excludedApps.push(Apps.Gmail)
      }
      if (!isSlackConnected) {
        excludedApps.push(Apps.Slack)
      }
    } catch (error) {
      // If no Slack connector is found, this is normal - exclude Slack from search
      // Only log as debug since this is expected behavior for users without Slack
      this.logger.debug(
        `No Slack connector found for user ${email}, excluding Slack from search`,
      )
      excludedApps.push(Apps.Slack)
    }

    let { yql, profile } = this.HybridDefaultProfileAppEntityCounts(
      limit,
      timestampRange ?? null,
      [], // notInMailLabels
      excludedApps, // excludedApps as fourth parameter
    )

    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
    }
    try {
      return await this.vespa.groupSearch(hybridDefaultPayload)
    } catch (error) {
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: this.getSchemaSources(),
      })
    }
  }

  searchVespa = async (
    query: string,
    email: string,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      timestampRange = null,
      excludedIds = [],
      notInMailLabels = [],
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
      recencyDecayRate = 0.02,
      isIntentSearch = false,
      intent = {},
      isSlackConnected,
      isCalendarConnected,
      isDriveConnected,
      isGmailConnected,
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    // either no prod config, or prod call errored
    return await this._searchVespa(query, email, app, entity, {
      alpha,
      limit,
      offset,
      timestampRange,
      excludedIds,
      notInMailLabels,
      rankProfile,
      requestDebug,
      span,
      maxHits,
      recencyDecayRate,
      isIntentSearch,
      intent,
      isSlackConnected,
      isCalendarConnected,
      isDriveConnected,
      isGmailConnected,
    })
  }

  _searchVespa(
    query: string,
    email: string,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      timestampRange = null,
      excludedIds = [],
      notInMailLabels = [],
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
      recencyDecayRate = 0.02,
      isIntentSearch = false,
      intent = {},
      isSlackConnected = false,
      isCalendarConnected = false,
      isDriveConnected = false,
      isGmailConnected = false,
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> {
    // Determine the timestamp cutoff based on lastUpdated
    // const timestamp = lastUpdated ? getTimestamp(lastUpdated) : null
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    // Check if Slack sync job exists for the user (only for local vespa)
    let excludedApps: Apps[] = []
    try {
      if (!isDriveConnected) {
        excludedApps.push(Apps.GoogleDrive)
      }
      if (!isCalendarConnected) {
        excludedApps.push(Apps.GoogleCalendar)
      }
      if (!isGmailConnected) {
        excludedApps.push(Apps.Gmail)
      }
      if (!isSlackConnected) {
        excludedApps.push(Apps.Slack)
      }
    } catch (error) {
      // If no Slack connector is found, this is normal - exclude Slack from search
      // Only log as debug since this is expected behavior for users without Slack
      this.logger.debug(
        `No Slack connector found for user ${email}, excluding Slack from search`,
      )
      excludedApps.push(Apps.Slack)
    }

    let { yql, profile } = this.HybridDefaultProfile(
      limit,
      app,
      entity,
      rankProfile,
      timestampRange,
      excludedIds,
      notInMailLabels,
      excludedApps,
      intent,
    )

    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      "input.query(recency_decay_rate)": recencyDecayRate,
      "input.query(is_intent_search)": isIntentSearch ? 1.0 : 0.0,
      maxHits,
      hits: limit,
      timeout: "30s",
      ...(offset
        ? {
            offset,
          }
        : {}),
      ...(app ? { app } : {}),
      ...(entity ? { entity } : {}),
      ...(isDebugMode ? { "ranking.listFeatures": true, tracelevel: 4 } : {}),
    }

    span?.setAttribute("vespaPayload", JSON.stringify(hybridDefaultPayload))
    try {
      let result = this.vespa.search<VespaSearchResponse>(hybridDefaultPayload)
      return result
    } catch (error) {
      this.logger.error(`Search failed with error:`, error)
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: this.getSchemaSources(),
      })
    }
  }

  searchVespaInFiles = async (
    query: string,
    email: string,
    fileIds: string[],
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      notInMailLabels = [],
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    let { yql, profile } = this.HybridDefaultProfileInFiles(
      limit,
      rankProfile,
      fileIds,
      notInMailLabels,
    )

    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      maxHits,
      hits: limit,
      timeout: "30s",
      ...(offset
        ? {
            offset,
          }
        : {}),
      ...(isDebugMode ? { "ranking.listFeatures": true, tracelevel: 4 } : {}),
    }
    span?.setAttribute("vespaPayload", JSON.stringify(hybridDefaultPayload))
    return this.vespa
      .search<VespaSearchResponse>(hybridDefaultPayload)
      .catch((error) => {
        throw new ErrorPerformingSearch({
          cause: error as Error,
          sources: this.getSchemaSources(),
        })
      })
  }

  searchSlackInVespa = async (
    query: string,
    email: string,
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
      channelIds = [],
      threadId = undefined,
      userId = undefined,
      timestampRange = null,
    }: Partial<VespaQueryConfig> & {
      channelIds?: string[]
      threadId?: string
      userId?: string
    },
  ): Promise<VespaSearchResponse> => {
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    let { yql, profile } = this.HybridDefaultProfileForSlack(
      limit,
      rankProfile,
      channelIds,
      threadId,
      userId,
      timestampRange,
    )

    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      maxHits,
      hits: limit,
      timeout: "30s",
      ...(offset && { offset }),
      ...(isDebugMode && { "ranking.listFeatures": true, tracelevel: 4 }),
    }
    span?.setAttribute("vespaPayload", JSON.stringify(hybridDefaultPayload))
    return this.vespa
      .search<VespaSearchResponse>(hybridDefaultPayload)
      .catch((error) => {
        throw new ErrorPerformingSearch({
          cause: error as Error,
          sources: chatMessageSchema,
        })
      })
  }

  searchVespaThroughAgent = async (
    query: string,
    email: string,
    apps: Apps[] | null,
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    if (!query?.trim()) {
      throw new Error("Query cannot be empty")
    }

    if (!email?.trim()) {
      throw new Error("Email cannot be empty")
    }
    return {} as VespaSearchResponse
  }

  searchVespaAgent = async (
    query: string,
    email: string,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    Apps: Apps[] | null,
    {
      alpha = 0.5,
      limit = this.config.page,
      offset = 0,
      timestampRange = null,
      excludedIds = [],
      notInMailLabels = [],
      rankProfile = SearchModes.NativeRank,
      requestDebug = false,
      span = null,
      maxHits = 400,
      recencyDecayRate = 0.02,
      dataSourceIds = [], // Ensure dataSourceIds is destructured here
      intent = null,
      channelIds = [],
      driveIds = [], // docIds
      selectedItem = {},
      processedCollectionSelections = {},
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    // Determine the timestamp cutoff based on lastUpdated
    // const timestamp = lastUpdated ? getTimestamp(lastUpdated) : null
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    let { yql, profile } = this.HybridDefaultProfileForAgent(
      limit,
      app,
      entity,
      rankProfile,
      timestampRange,
      excludedIds,
      notInMailLabels,
      Apps,
      dataSourceIds, // Pass dataSourceIds here
      intent,
      channelIds,
      processedCollectionSelections, // Pass processedCollectionSelections
      driveIds,
      selectedItem,
    )

    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      "input.query(recency_decay_rate)": recencyDecayRate,
      maxHits,
      hits: limit,
      timeout: "30s",
      ...(offset
        ? {
            offset,
          }
        : {}),
      ...(app ? { app } : {}),
      ...(entity ? { entity } : {}),
      ...(isDebugMode ? { "ranking.listFeatures": true, tracelevel: 4 } : {}),
    }
    span?.setAttribute("vespaPayload", JSON.stringify(hybridDefaultPayload))
    return this.vespa
      .search<VespaSearchResponse>(hybridDefaultPayload)
      .catch((err: any) => {
        throw err
      })
      .catch((error: any) => {
        throw new ErrorPerformingSearch({
          cause: error as Error,
          sources: AllSources.join(", "),
        })
      })
  }

  GetDocument = async (schema: VespaSchema, docId: string) => {
    const opts = { namespace: this.config.namespace, docId, schema }
    return this.vespa.getDocument(opts).catch((error) => {
      this.logger.error(error, `Error fetching document docId: ${docId}`)
      throw new Error(getErrorMessage(error))
    })
  }

  IfMailDocExist = async (email: string, docId: string): Promise<boolean> => {
    return this.vespa.ifMailDocExist(email, docId).catch((error) => {
      this.logger.error(
        error,
        `Error checking if document docId: ${docId} exists`,
      )
      return false
    })
  }

  GetDocumentsByDocIds = async (
    docIds: string[],
    generateAnswerSpan: Span,
  ): Promise<VespaSearchResponse> => {
    const opts = {
      namespace: this.config.namespace,
      docIds,
      generateAnswerSpan,
    }
    return this.vespa.getDocumentsByOnlyDocIds(opts).catch((error) => {
      this.logger.error(error, `Error fetching document docIds: ${docIds}`)
      throw new Error(getErrorMessage(error))
    })
  }

  /**
   * Fetches a single random document from a specific schema.
   */
  GetRandomDocument = async (
    namespace: string,
    schema: string,
    cluster: string,
  ): Promise<any | null> => {
    return this.vespa
      .getRandomDocument(namespace, schema, cluster)
      .catch((error) => {
        this.logger.error(
          error,
          `Error fetching random document for schema ${schema}`,
        )
        throw new Error(getErrorMessage(error))
      })
  }

  GetDocumentWithField = async (
    fieldName: string,
    schema: VespaSchema,
    limit: number = 100,
    offset: number = 0,
  ): Promise<VespaSearchResponse> => {
    const opts = { namespace: this.config.namespace, schema }
    return this.vespa
      .getDocumentsWithField(fieldName, opts, limit, offset)
      .catch((error) => {
        this.logger.error(
          error,
          `Error fetching documents with field: ${fieldName}`,
        )
        throw new Error(getErrorMessage(error))
      })
  }

  UpdateDocumentPermissions = async (
    schema: VespaSchema,
    docId: string,
    updatedPermissions: string[],
  ) => {
    const opts = { namespace: this.config.namespace, docId, schema }
    return this.vespa
      .updateDocumentPermissions(updatedPermissions, opts)
      .catch((error) => {
        this.logger.error(
          error,
          `Error updating document permissions for docId: ${docId}`,
        )
        throw new Error(getErrorMessage(error))
      })
  }

  UpdateEventCancelledInstances = async (
    schema: VespaSchema,
    docId: string,
    updatedCancelledInstances: string[],
  ) => {
    const opts = { namespace: this.config.namespace, docId, schema }
    return this.vespa
      .updateCancelledEvents(updatedCancelledInstances, opts)
      .catch((error) => {
        this.logger.error(
          error,
          `Error updating event cancelled instances for docId: ${docId}`,
        )
        throw new Error(getErrorMessage(error))
      })
  }

  UpdateDocument = async (
    schema: VespaSchema,
    docId: string,
    updatedFields: Record<string, any>,
  ) => {
    const opts = { namespace: this.config.namespace, docId, schema }

    return this.vespa.updateDocument(updatedFields, opts).catch((error) => {
      this.logger.error(error, `Error updating document for docId: ${docId}`)
      throw new Error(getErrorMessage(error))
    })
  }

  DeleteDocument = async (docId: string, schema: VespaSchema) => {
    const opts = { namespace: this.config.namespace, docId, schema }
    return this.vespa.deleteDocument(opts).catch((error) => {
      this.logger.error(error, `Error deleting document for docId: ${docId}`)
      throw new Error(getErrorMessage(error))
    })
  }

  ifDocumentsExist = async (
    docIds: string[],
  ): Promise<Record<string, { exists: boolean; updatedAt: number | null }>> => {
    return this.vespa.ifDocumentsExist(docIds).catch((error) => {
      this.logger.error(error, `Error checking if documents exist: ${docIds}`)
      throw new Error(getErrorMessage(error))
    })
  }

  ifMailDocumentsExist = async (
    mailIds: string[],
  ): Promise<
    Record<
      string,
      {
        docId: string
        exists: boolean
        updatedAt: number | null
        userMap: Record<string, string>
      }
    >
  > => {
    return this.vespa.ifMailDocumentsExist(mailIds).catch((error) => {
      this.logger.error(
        error,
        `Error checking if mail documents exist: ${mailIds}`,
      )
      throw new Error(getErrorMessage(error))
    })
  }

  ifDocumentsExistInChatContainer = async (
    docIds: string[],
  ): Promise<
    Record<
      string,
      { exists: boolean; updatedAt: number | null; permissions: string[] }
    >
  > => {
    return this.vespa.ifDocumentsExistInChatContainer(docIds).catch((error) => {
      this.logger.error(
        error,
        `Error checking if documents exist in chat container: ${docIds}`,
      )
      throw new Error(getErrorMessage(error))
    })
  }

  ifDocumentsExistInSchema = async (
    schema: string,
    docIds: string[],
  ): Promise<Record<string, { exists: boolean; updatedAt: number | null }>> => {
    return this.vespa
      .ifDocumentsExistInSchema(schema, docIds)
      .catch((error) => {
        this.logger.error(
          error,
          `Error checking if documents exist in schema: ${schema}`,
        )
        throw new Error(getErrorMessage(error))
      })
  }

  getNDocuments = async (n: number) => {
    // Encode the YQL query to ensure it's URL-safe
    const yql = encodeURIComponent(
      `select * from sources ${fileSchema} where true`,
    )

    // Construct the search URL with necessary query parameters
    const url = `${this.vespaEndpoint}/search/?yql=${yql}&hits=${n}&cluster=${this.config.cluster}`

    try {
      const response: Response = await fetch(url, {
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

      const data = await response.json()

      return data
    } catch (error) {
      const errMessage = getErrorMessage(error)
      this.logger.error(
        `Error retrieving document count: , ${errMessage}`,
        error,
      )
      throw new ErrorRetrievingDocuments({
        cause: error as Error,
        sources: "file",
      })
    }
  }

  hashQuery = (query: string) => {
    return crypto.createHash("sha256").update(query.trim()).digest("hex")
  }

  updateUserQueryHistory = async (query: string, owner: string) => {
    const docId = `query_id-${this.hashQuery(query + owner)}`
    const timestamp = new Date().getTime()

    try {
      const docExist = await this.getDocumentOrNull(userQuerySchema, docId)

      if (docExist) {
        const docFields = docExist.fields as VespaUserQueryHistory
        const timeSinceLastUpdate = timestamp - docFields.timestamp
        if (timeSinceLastUpdate > this.config.userQueryUpdateInterval) {
          await this.UpdateDocument(userQuerySchema, docId, {
            count: docFields.count + 1,
            timestamp,
          })
        } else {
          this.logger.warn(`Skipping update for ${docId}: Under time interval`)
        }
      } else {
        await this.insert(
          { docId, query_text: query, timestamp, count: 1, owner: owner },
          userQuerySchema,
        )
      }
    } catch (error) {
      const errMsg = getErrorMessage(error)
      this.logger.error(`Update user query error: ${errMsg}`, error)
      throw new Error("Failed to update user query history")
    }
  }

  getDocumentOrNull = async (schema: VespaSchema, docId: string) => {
    try {
      return await this.GetDocument(schema, docId)
    } catch (error) {
      const errMsg = getErrorMessage(error)
      if (errMsg.includes("404 Not Found")) {
        this.logger.warn(`Document ${docId} does not exist`)
        return null
      }

      throw error
    }
  }

  searchUsersByNamesAndEmails = async (
    mentionedNames: string[],
    mentionedEmails: string[],
    limit: number = 10,
  ): Promise<VespaSearchResponse> => {
    // Construct YQL conditions for names and emails
    const nameConditions = mentionedNames.map((name) => {
      // For fuzzy search
      return `(name_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy("${name}")))`
      // For exact match, use:
      // return `(name contains "${name}")`;
    })

    const emailConditions = mentionedEmails.map((email) => {
      // For fuzzy search
      return `(email_fuzzy contains ({maxEditDistance: 2, prefix: true} fuzzy("${email}")))`
      // For exact match, use:
      // return `(email contains "${email}")`;
    })

    // Combine all conditions with OR operator
    const allConditions = [...nameConditions, ...emailConditions].join(" or ")

    // Build the full YQL query
    const yqlQuery = `select * from sources ${userSchema} where (${allConditions});`

    const searchPayload = {
      yql: yqlQuery,
      hits: limit,
      "ranking.profile": "default",
    }

    return this.vespa.getUsersByNamesAndEmails(searchPayload).catch((error) => {
      this.logger.error(
        error,
        `Error fetching users by names and emails: ${searchPayload}`,
      )
      throw new Error(getErrorMessage(error))
    }) as Promise<VespaSearchResponse>
  }

  /**
   * Helper function to calculate the timestamp based on LastUpdated value.
   */
  getTimestamp = (lastUpdated: string): number | null => {
    const now = new Date().getTime() // Convert current time to epoch seconds
    switch (lastUpdated) {
      case "pastDay":
        return now - 24 * 60 * 60 * 1000
      case "pastWeek":
        return now - 7 * 24 * 60 * 60 * 1000
      case "pastMonth":
        return now - 30 * 24 * 60 * 60 * 1000
      case "pastYear":
        return now - 365 * 24 * 60 * 60 * 1000
      case "anytime":
      default:
        return null
    }
  }

  //  searchEmployeesViaName = async (
  //   name: string,
  //   email: string,
  //   limit = config.page,
  //   offset?: number,
  // ): Promise<VespaSearchResponse> => {
  //   const url = `${vespaEndpoint}/search/`

  //   const yqlQuery = `
  //       select * from sources user
  //       where name contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))`

  //   const hybridDefaultPayload = {
  //     yql: yqlQuery,
  //     query: name,
  //     email,
  //     "ranking.profile": HybridDefaultProfile(limit).profile,
  //     "input.query(e)": "embed(@query)",
  //     hits: limit,
  //     alpha: 0.5,
  //     ...(offset
  //       ? {
  //           offset,
  //         }
  //       : {}),
  //     variables: {
  //       query,
  //     },
  //   }
  //   try {
  //     const response = await fetch(url, {
  //       method: "POST",
  //       headers: {
  //         "Content-Type": "application/json",
  //       },
  //       body: JSON.stringify(hybridDefaultPayload),
  //     })
  //     if (!response.ok) {
  //       const errorText = response.statusText
  //       throw new Error(
  //         `Failed to fetch documents: ${response.status} ${response.statusText} - ${errorText}`,
  //       )
  //     }

  //     const data = await response.json()
  //     return data
  //   } catch (error) {
  //     this.logger.error(`Error performing search:, ${error}`)
  //     throw new ErrorPerformingSearch({
  //       cause: error as Error,
  //       sources: AllSources,
  //     })
  //   }
  // }

  getItems = async (params: GetItemsParams): Promise<VespaSearchResponse> => {
    const {
      schema,
      app,
      entity,
      timestampRange,
      limit = this.config.page,
      offset = 0,
      email,
      excludedIds, // Added excludedIds here
      asc,
      intent,
      channelIds,
    } = params

    const schemas = Array.isArray(schema) ? schema : [schema]
    const schemasString = schemas.join(", ")
    // Construct conditions based on parameters
    let conditions: string[] = []

    // App condition
    if (Array.isArray(app) && app.length > 0) {
      conditions.push(
        `(${app.map((a) => `app contains '${escapeYqlValue(a)}'`).join(" or ")})`,
      )
    } else if (!Array.isArray(app) && app) {
      conditions.push(`app contains '${escapeYqlValue(app)}'`)
    }

    if (app === Apps.Slack && channelIds && channelIds.length > 0) {
      const channelIdConditions = channelIds
        .map((id) => `channelId contains '${escapeYqlValue(id)}'`)
        .join(" or ")
      conditions.push(`(${channelIdConditions})`)
    }

    // Entity condition
    if (Array.isArray(entity) && entity.length > 0) {
      conditions.push(
        `(${entity
          .map((e) => `entity contains '${escapeYqlValue(e)}'`)
          .join(" or ")})`,
      )
    } else if (!Array.isArray(entity) && entity) {
      conditions.push(`entity contains '${escapeYqlValue(entity)}'`)
    }

    // Permissions or owner condition based on schema
    if (schemas.length === 1 && schemas[0] === dataSourceFileSchema) {
      // Temporal fix for datasoure selection
    } else if (!schemas.includes(userSchema)) {
      conditions.push(`permissions contains '${email}'`)
    } else {
      // For user schema
      if (
        app !== Apps.GoogleWorkspace ||
        (Array.isArray(app) && !app.includes(Apps.GoogleWorkspace))
      ) {
        conditions.push(`owner contains '${email}'`)
      }
    }

    let timestampField = []

    // Choose appropriate timestamp field based on schema
    if (
      schemas.includes(mailSchema) ||
      schemas.includes(mailAttachmentSchema)
    ) {
      timestampField.push("timestamp")
    } else if (
      schemas.includes(fileSchema) ||
      schemas.includes(chatMessageSchema)
    ) {
      timestampField.push("updatedAt")
    } else if (schemas.includes(eventSchema)) {
      timestampField.push("startTime")
    } else if (schemas.includes(userSchema)) {
      timestampField.push("creationTime")
    } else {
      timestampField.push("updatedAt")
    }

    // Timestamp conditions
    if (timestampRange) {
      let timeConditions: string[] = []
      let fieldForRange = timestampField // Use default field unless orderBy overrides

      if (timestampRange.from) {
        timeConditions.push(
          `${fieldForRange.map((field) => `${field} >= ${new Date(timestampRange.from!).getTime()}`).join(" or ")}`,
        )
      }
      if (timestampRange.to) {
        timeConditions.push(
          `${fieldForRange.map((field) => `${field} <= ${new Date(timestampRange.to!).getTime()}`).join(" or ")}`,
        )
      }
      if (timeConditions.length > 0) {
        conditions.push(`(${timeConditions.join(" and ")})`)
      }
    }

    // Excluded IDs condition
    if (excludedIds && excludedIds.length > 0) {
      const exclusionCondition = excludedIds
        .map((id) => `docId contains '${id}'`)
        .join(" or ")
      conditions.push(`!(${exclusionCondition})`)
    }

    // Intent-based conditions - modular approach for different apps
    if (intent) {
      this.logger.debug("Processing intent-based filtering", {
        intent,
        app,
        entity,
        schema,
      })

      // Handle Gmail intent filtering
      if (
        app === Apps.Gmail &&
        entity === MailEntity.Email &&
        schema === mailSchema
      ) {
        const gmailIntentConditions = processGmailIntent(intent, this.logger)
        if (gmailIntentConditions.length > 0) {
          conditions.push(...gmailIntentConditions)
          this.logger.debug(
            `Added Gmail intent conditions: ${gmailIntentConditions.join(" and ")}`,
          )
        } else {
          this.logger.debug(
            "Gmail intent provided but contains only names/non-specific identifiers - skipping intent filtering",
            { intent },
          )
        }
      }

      // Future: Handle Slack intent filtering
      // else if (app === Apps.Slack && entity === SlackEntity.Message && schema === chatMessageSchema) {
      //   const slackIntentConditions = processSlackIntent(intent)
      //   if (slackIntentConditions.length > 0) {
      //     conditions.push(...slackIntentConditions)
      //     this.logger.debug(`Added Slack intent conditions: ${slackIntentConditions.join(" and ")}`)
      //   }
      // }

      // Future: Handle other apps...
    }

    // Combine conditions
    const whereClause =
      conditions.length > 0 ? `where ${conditions.join(" and ")}` : "where true"

    const orderByClause = timestampField
      ? `order by ${timestampField} ${asc ? "asc" : "desc"}`
      : ""

    // Construct YQL query with proper clause ordering and spacing
    let yql = `select * from sources ${schema} ${whereClause}`

    if (orderByClause) {
      yql += ` ${orderByClause}`
    }

    this.logger.info(`[getItems] YQL Query: ${yql}`)
    this.logger.info(`[getItems] Query Details:`, {
      schema,
      app,
      entity,
      limit,
      offset,
      intentProvided: !!intent,
      conditions: conditions.length > 0 ? conditions : "none",
    })

    const searchPayload = {
      yql,
      "ranking.profile": "unranked",
      timeout: "30s",
    }

    return this.vespa.getItems(searchPayload).catch((error) => {
      const searchError = new ErrorPerformingSearch({
        cause: error as Error,
        sources: JSON.stringify(schema),
        message: `getItems failed for schema ${schema}`,
      })
      this.logger.error(searchError, "Error in getItems function")
      throw searchError
    }) as Promise<VespaSearchResponse>
  }

  // --- DataSource and DataSourceFile Specific Functions ---

  insertDataSource = async (document: VespaDataSource): Promise<void> => {
    try {
      await this.insert(document as Inserts, datasourceSchema)
      this.logger.info(`DataSource ${document.docId} inserted successfully`)
    } catch (error) {
      this.logger.error(`Error inserting DataSource ${document.docId}`, error)
      throw error
    }
  }

  insertDataSourceFile = async (
    document: VespaDataSourceFile,
  ): Promise<void> => {
    try {
      await this.insert(document as Inserts, dataSourceFileSchema)
      this.logger.info(`DataSourceFile ${document.docId} inserted successfully`)
    } catch (error) {
      this.logger.error(
        `Error inserting DataSourceFile ${document.docId}`,
        error,
      )
      throw error
    }
  }

  getDataSourceByNameAndCreator = async (
    name: string,
    createdByEmail: string,
  ): Promise<VespaDataSourceSearch | null> => {
    const yql = `select * from ${datasourceSchema} where name contains @name and createdBy contains @email limit 1`

    const payload = {
      yql,
      name,
      email: createdByEmail,
      hits: 1,
      "ranking.profile": "unranked",
      "presentation.summary": "default",
    }

    const parseResult = (
      res: VespaSearchResponse,
    ): VespaDataSourceSearch | null => {
      const first = res?.root?.children?.[0]
      return first?.fields
        ? (first.fields as unknown as VespaDataSourceSearch)
        : null
    }

    const errorMsg = `Error fetching DataSource by name "${name}" and creator "${createdByEmail}"`

    try {
      const response = await this.vespa.search(payload)
      return parseResult(response as VespaSearchResponse)
    } catch (error) {
      this.logger.error(
        `Vespa failed for DataSource by name="${name}", email="${createdByEmail}"`,
        error,
      )
      throw new ErrorPerformingSearch({
        message: errorMsg,
        cause: error as Error,
        sources: datasourceSchema,
      })
    }
  }

  getDataSourcesByCreator = async (
    createdByEmail: string,
    limit: number = 100,
  ): Promise<VespaSearchResponse> => {
    const yql = `select * from ${datasourceSchema} where createdBy contains @email limit ${limit}`
    const payload = {
      yql,
      email: createdByEmail,
      hits: limit,
      "ranking.profile": "unranked",
      "presentation.summary": "default",
    }

    try {
      return await this.vespa.search<VespaSearchResponse>(payload)
    } catch (error) {
      const message = `Error fetching DataSources for creator "${createdByEmail}"`

      this.logger.error(message, error)
      throw new ErrorPerformingSearch({
        message,
        cause: error as Error,
        sources: datasourceSchema,
      })
    }
  }

  checkIfDataSourceFileExistsByNameAndId = async (
    fileName: string,
    dataSourceId: string,
    uploadedBy: string,
  ): Promise<boolean> => {
    const yql = `
    select * 
    from sources ${dataSourceFileSchema} 
    where fileName contains @fileName and dataSourceId contains @dataSourceId and uploadedBy contains @uploadedBy 
    limit 1
  `

    const payload = {
      yql,
      fileName,
      dataSourceId,
      uploadedBy,
      hits: 1,
      "ranking.profile": "unranked",
    }

    const exists = (res: VespaSearchResponse) => !!res?.root?.children?.length

    const errorMsg = `Error checking if file "${fileName}" exists for DataSource ID "${dataSourceId}" and user "${uploadedBy}"`

    try {
      this.logger.debug("Checking if datasource file exists by name and ID", {
        payload,
      })
      const response = await this.vespa.search<VespaSearchResponse>(payload)
      return exists(response)
    } catch (error) {
      this.logger.error(errorMsg, error)
      throw new ErrorPerformingSearch({
        message: errorMsg,
        cause: error as Error,
        sources: dataSourceFileSchema,
      })
    }
  }

  fetchAllDataSourceFilesByName = async (
    dataSourceName: string,
    userEmail: string,
    concurrency = 3,
    batchSize = 400,
  ): Promise<VespaSearchResult[] | null> => {
    const countPayload = {
      yql: `
      select * 
      from sources ${dataSourceFileSchema} 
      where dataSourceName contains @dataSourceName and uploadedBy contains @userEmail 
    `,
      dataSourceName,
      userEmail,
      hits: 0,
      timeout: "20s",
      "presentation.summary": "count",
      "ranking.profile": "unranked",
    }

    let totalCount: number

    try {
      const countResponse =
        await this.vespa.search<VespaSearchResponse>(countPayload)
      totalCount = countResponse.root?.fields?.totalCount ?? 0
      this.logger.info(`Found ${totalCount} total files`)
      if (totalCount === 0) {
        return null
      }
    } catch (error) {
      this.logger.error("Failed to get total count of files", error)
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: dataSourceFileSchema,
        message: "Failed to get total count",
      })
    }

    const batchPayloads = []
    for (let offset = 0; offset < totalCount; offset += batchSize) {
      const payload = {
        yql: `
        select * 
        from sources ${dataSourceFileSchema} 
        where dataSourceName contains @dataSourceName and uploadedBy contains @userEmail 
        order by createdAt desc
      `,
        dataSourceName,
        userEmail,
        hits: Math.min(batchSize, totalCount - offset),
        offset,
        timeout: "30s",
        "ranking.profile": "unranked",
        "presentation.summary": "default",
        maxHits: 1000000,
        maxOffset: 1000000,
      }
      batchPayloads.push(payload)
    }

    this.logger.debug("Prepared batch payloads for Vespa", batchPayloads)

    this.logger.info(
      `Fetching all batches (${batchPayloads.length}) with concurrency=${concurrency}`,
    )

    const limiter = pLimit(concurrency)

    const results = await Promise.all(
      batchPayloads.map((payload, idx) =>
        limiter(async () => {
          this.logger.debug(`Fetching batch ${idx + 1}/${batchPayloads.length}`)
          const res = await this.vespa
            .search<VespaSearchResponse>(payload)
            .catch(async (err) => {
              throw err
            })
          return res
        }),
      ),
    )

    const allChildren = results.flatMap((r) => r.root.children ?? [])

    return allChildren
  }

  SlackHybridProfile = (
    hits: number,
    entity: Entity | null,
    profile: SearchModes = SearchModes.NativeRank,
    timestampRange?: { to: number | null; from: number | null } | null,
    channelId?: string,
    userId?: string,
  ): YqlProfile => {
    // Helper function to build timestamp conditions
    const buildTimestampConditions = (fromField: string, toField: string) => {
      const conditions: string[] = []
      if (timestampRange?.from) {
        conditions.push(`${fromField} >= ${timestampRange.from}`)
      }
      if (timestampRange?.to) {
        conditions.push(`${toField} <= ${timestampRange.to}`)
      }
      return conditions.join(" and ")
    }

    // Helper function to build entity filter
    const buildEntityFilter = () => {
      return entity ? "and entity contains @entity" : ""
    }

    // Helper function to build channel filter
    const buildChannelFilter = () => {
      return channelId ? "and channelId contains @channelId" : ""
    }

    // Helper function to build user filter
    const buildUserFilter = () => {
      return userId ? "and userId contains @userId" : ""
    }

    // Build Slack YQL
    const buildSlackYQL = () => {
      const timestampCondition = timestampRange
        ? buildTimestampConditions("createdAt", "createdAt")
        : ""
      const entityFilter = buildEntityFilter()
      const channelFilter = buildChannelFilter()
      const userFilter = buildUserFilter()

      return `
      (
        (
          ({targetHits:${hits}} userInput(@query))
          or
          ({targetHits:${hits}} nearestNeighbor(text_embeddings, e))
        )
        ${timestampCondition ? `and (${timestampCondition})` : ""}
        and permissions contains @email
        ${entityFilter}
        ${channelFilter}
        ${userFilter}
      )`
    }

    const combinedQuery = buildSlackYQL()
    const sources = [chatMessageSchema] // Only chat message schema for Slack

    return {
      profile: profile,
      yql: `
    select *
    from sources ${sources.join(", ")} 
    where
    (
      (
        ${combinedQuery}
      )
    )
    ;
    `,
    }
  }

  SearchVespaThreads = async (
    threadIdsInput: string[],
    generateAnswerSpan: Span,
  ): Promise<VespaSearchResponse> => {
    const validThreadIds = threadIdsInput.filter(
      (id) => typeof id === "string" && id.length > 0,
    )

    if (validThreadIds.length === 0) {
      this.logger.warn("SearchVespaThreads called with no valid threadIds.")
      return {
        root: {
          id: "nullss",
          relevance: 0,
          fields: { totalCount: 0 },
          coverage: {
            coverage: 0,
            documents: 0,
            full: true,
            nodes: 0,
            results: 0,
            resultsFull: 0,
          },
          children: [],
        },
      }
    }

    return this.vespa.getDocumentsBythreadId(validThreadIds).catch((error) => {
      this.logger.error(
        error,
        `Error fetching documents by threadIds: ${validThreadIds.join(", ")}`,
      )
      const errMessage = getErrorMessage(error)
      throw new Error(errMessage)
    }) as Promise<VespaSearchResponse>
  }

  SearchEmailThreads = async (
    threadIdsInput: string[],
    email: string,
  ): Promise<VespaSearchResponse> => {
    const validThreadIds = threadIdsInput.filter(
      (id) => typeof id === "string" && id.length > 0,
    )
    return this.vespa
      .getEmailsByThreadIds(validThreadIds, email)
      .catch((error) => {
        this.logger.error(
          error,
          `Error fetching emails by threadIds: ${validThreadIds.join(", ")}`,
        )
        const errMessage = getErrorMessage(error)
        throw new Error(errMessage)
      })
  }

  getThreadItems = async (
    params: GetThreadItemsParams & { filterQuery?: string },
  ): Promise<VespaSearchResponse> => {
    const {
      entity = SlackEntity.Message,
      timestampRange = null,
      limit = this.config.page,
      offset = 0,
      email,
      userEmail = null,
      asc = true,
      channelName = null,
      filterQuery = null,
    } = params
    const chatMessageSchema = "chat_message"

    // Handle timestamp range normalization
    if (timestampRange) {
      if (timestampRange.from) {
        timestampRange.from = dateToUnixTimestamp(timestampRange.from, false)
      }
      if (timestampRange.to) {
        timestampRange.to = dateToUnixTimestamp(timestampRange.to, true)
      }
    }

    let channelId: string | undefined
    let userId: string | undefined

    // Fetch channelId
    if (channelName) {
      try {
        const resp = (await this.vespa.getChatContainerIdByChannelName(
            channelName,
          )) as any,
          channelId = resp?.root?.children?.[0]?.fields?.docId
      } catch (e) {
        this.logger.error(
          `Could not fetch channelId for channel: ${channelName}`,
          e,
        )
      }
    }

    // Fetch userId
    if (userEmail) {
      try {
        const resp = this.vespa.getChatUserByEmail(userEmail) as any,
          userId = resp?.root?.children?.[0]?.fields?.docId
      } catch (e) {
        this.logger.error(`Could not fetch userId for user: ${userEmail}`, e)
      }
    }

    // Hybrid filterQuery-based search
    if (filterQuery) {
      const { yql, profile } = this.SlackHybridProfile(
        limit,
        SlackEntity.Message,
        SearchModes.NativeRank,
        timestampRange,
        channelId,
        userId,
      )

      const hybridPayload = {
        yql,
        query: filterQuery,
        email: email,
        "ranking.profile": profile,
        "input.query(e)": "embed(@query)",
        "input.query(alpha)": 0.5,
        "input.query(recency_decay_rate)": 0.1,
        maxHits: limit,
        hits: limit,
        timeout: "20s",
        ...(offset && { offset }),
        ...(entity && { entity }),
        ...(channelId && { channelId }),
        ...(userId && { userId }),
      }

      try {
        return await this.vespa.search<VespaSearchResponse>(hybridPayload)
      } catch (error) {
        this.logger.error(`Vespa hybrid search failed`, error)
        throw new ErrorPerformingSearch({
          cause: error as Error,
          sources: chatMessageSchema,
        })
      }
    }

    // Plain YQL search
    const conditions: string[] = []

    if (entity) conditions.push(`entity contains "${entity}"`)
    if (userEmail) conditions.push(`permissions contains "${userEmail}"`)
    if (channelId) conditions.push(`channelId contains "${channelId}"`)
    if (userId) conditions.push(`userId contains "${userId}"`)

    const timestampField = "createdAt"

    const buildTimestampConditions = (fromField: string, toField: string) => {
      const timestampConditions: string[] = []
      if (timestampRange?.from) {
        timestampConditions.push(`${fromField} >= '${timestampRange.from}'`)
      }
      if (timestampRange?.to) {
        timestampConditions.push(`${toField} <= '${timestampRange.to}'`)
      }
      return timestampConditions
    }

    if (timestampRange) {
      const timestampConditions = buildTimestampConditions(
        timestampField,
        timestampField,
      )
      conditions.push(...timestampConditions)
    }

    const whereClause = conditions.length
      ? `where ${conditions.join(" and ")}`
      : ""
    const orderClause = `order by createdAt ${asc ? "asc" : "desc"}`
    const yql = `select * from sources ${chatMessageSchema} ${whereClause} ${orderClause} limit ${limit} offset ${offset}`

    const payload = {
      yql,
      "ranking.profile": "unranked",
    }

    try {
      return (await this.vespa.getItems(
        payload,
      )) as Promise<VespaSearchResponse>
    } catch (error) {
      this.logger.error(`Vespa search error`, error)
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: chatMessageSchema,
      })
    }
  }

  getSlackUserDetails = async (
    userEmail: string,
  ): Promise<VespaSearchResponse> => {
    return this.vespa.getChatUserByEmail(userEmail).catch((error) => {
      this.logger.error(
        `Could not fetch the userId with user email ${userEmail}`,
      )
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: chatUserSchema,
      })
    })
  }

  getFolderItems = async (
    docIds: string[],
    schema: string,
    entity: string,
    email: string,
  ) => {
    try {
      const resp = this.vespa.getFolderItem(
        docIds,
        schema,
        entity,
        email,
      ) as Promise<VespaSearchResponse>
      return resp
    } catch (error) {
      this.logger.error(
        `Error fetching folderitem by docIds: ${docIds.join(", ")}`,
        error,
      )
      const errMessage = getErrorMessage(error)
      throw new Error(errMessage)
    }
  }

  /**
   * RAG-based search function for collection source only
   * This function performs semantic search using both text matching and vector embeddings
   * on KnowledgeBase (collection) schema only, with optional ID filtering
   * @param query The search query for RAG
   * @param docIds Optional array of document IDs to filter results (if provided, search only within these docs)
   * @param limit Maximum number of results to return (default: 10)
   * @param offset Offset for pagination (default: 0)
   * @param alpha Balance between text search and vector search (0.0 = only vector, 1.0 = only text, default: 0.5)
   * @param rankProfile Ranking profile to use (default: NativeRank)
   * @returns Promise<VespaSearchResponse> containing the matching documents from collection source
   */
  searchCollectionRAG = async (
    query: string,
    docIds?: string[],
    parentDocIds?: string[],
    limit: number = 10,
    offset: number = 0,
    alpha: number = 0.5,
    rankProfile: SearchModes = SearchModes.NativeRank,
  ): Promise<VespaSearchResponse> => {
    if (!query || query.trim().length === 0) {
      this.logger.warn("searchCollectionRAG called with empty query")
      throw new ErrorPerformingSearch({
        cause: new Error("empty query string"),
        sources: KbItemsSchema,
      })
    }

    // Build optional docId filtering conditions
    let docIdFilter = ""
    if (docIds && docIds.length > 0) {
      const docIdConditions = docIds
        .map((id) => `docId contains '${escapeYqlValue(id.trim())}'`)
        .join(" or ")
      docIdFilter = `and (${docIdConditions})`
    }
    let parentDocIdFilter = ""
    if (parentDocIds && parentDocIds.length > 0) {
      const parentDocIdConditions = parentDocIds
        .map((id) => `clFd contains '${escapeYqlValue(id.trim())}'`)
        .join(" or ")
      parentDocIdFilter = `and (${parentDocIdConditions})`
    }

    // Construct RAG YQL query - hybrid search with both text and vector search
    // This combines BM25 text search with vector similarity search
    const yqlQuery = `select * from sources ${KbItemsSchema} where (
      (
        ({targetHits:${limit}} userInput(@query))
        or
        ({targetHits:${limit}} nearestNeighbor(chunk_embeddings, e))
      )
      ${docIdFilter}
      ${parentDocIdFilter}
    )`

    const searchPayload = {
      yql: yqlQuery,
      query: query.trim(),
      "ranking.profile": rankProfile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      hits: limit,
      offset,
      timeout: "30s",
    }

    try {
      const response =
        await this.vespa.search<VespaSearchResponse>(searchPayload)
      this.logger.info(
        `[searchCollectionRAG] Found ${response.root?.children?.length || 0} documents for query: "${query.trim()}"`,
      )

      return response
    } catch (error) {
      const searchError = new ErrorPerformingSearch({
        cause: error as Error,
        sources: KbItemsSchema,
        message: `searchCollectionRAG failed for query: "${query.trim()}"${docIds ? ` with docIds: ${docIds.join(", ")}` : ""}`,
      })
      this.logger.error(searchError, "Error in searchCollectionRAG function")
      throw searchError
    }
  }
}
