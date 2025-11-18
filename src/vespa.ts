import {
  Apps,
  GoogleApps,
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
  type Span,
  SlackEntity,
  chatContainerSchema,
  KbItemsSchema,
  type CollectionVespaIds,
  AttachmentEntity,
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
  Inserts,
  VespaQueryConfig,
  GetItemsParams,
  MailParticipant,
  EventStatusType,
  SearchGoogleAppsParams,
  SearchSlackParams,
  VespaChatMessage,
  VespaChatUser,
  AppFilter,
  VespaChatContainer,
} from "./types"
import { SearchModes } from "./types"
import {
  dateToUnixTimestamp,
  escapeYqlValue,
  formatYqlToReadable,
  getErrorMessage,
  getGmailParticipantsConditions,
  isValidEmail,
  isValidTimestampRange,
  normalizeTimestamp,
  vespaEmptyResponse,
} from "./utils"
import { YqlBuilder } from "./yql/yqlBuilder"
import { And, Or, FuzzyContains } from "./yql/conditions"
import {
  or,
  and,
  userInput,
  nearestNeighbor,
  timestamp,
  contains,
  matches,
  greaterThanOrEqual,
  lessThanOrEqual,
  fuzzy,
  not,
} from "./yql"
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
import { YqlCondition } from "./yql/types"
import { off } from "process"
import { is } from "zod/v4/locales"

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
] as VespaSchema[]

export class VespaService {
  private logger: ILogger
  private config: VespaConfig
  private vespa: VespaClient
  private schemaSources: VespaSchema[]
  constructor(dependencies: VespaDependencies) {
    this.logger = dependencies.logger.child({ module: "vespa" })
    this.config = dependencies.config
    this.schemaSources = dependencies.sourceSchemas || AllSources
    // Initialize Vespa clients
    this.vespa = new VespaClient(this.logger, this.config)
  }

  getSchemaSources(): VespaSchema[] {
    return this.schemaSources
  }

  getSchemaSourcesString(): string {
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
          sources: this.getSchemaSourcesString(),
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
    const sources = [
      ...new Set<VespaSchema>([...this.getSchemaSources(), userQuerySchema]),
    ].filter((s) => s !== chatMessageSchema)

    // Construct the YQL query for fuzzy prefix matching with maxEditDistance:2
    // the drawback here is that for user field we will get duplicates, for the same
    // email one contact and one from user directory
    const yql = YqlBuilder.create({ email, requirePermissions: true })
      .from(sources)
      .where(
        or([
          fuzzy("title_fuzzy", "@query"),
          fuzzy("name_fuzzy", "@query"),
          fuzzy("email_fuzzy", "@query"),
          // skip permission check for google workspace app
          Or.withoutPermissions([
            And.withoutPermissions([
              fuzzy("name_fuzzy", "@query"),
              contains("app", Apps.GoogleWorkspace),
            ]),
            And.withoutPermissions([
              fuzzy("email_fuzzy", "@query"),
              contains("app", Apps.GoogleWorkspace),
            ]),
          ]),
          fuzzy("subject_fuzzy", "@query"),
          fuzzy("query_text", "@query"),
        ]),
      )
    const yqlQuery = yql.build()
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
    })
  }

  handleAppsNotInYql = (app: Apps | null, includedApp: Apps[]) => {
    this.logger.error(`${app} is not supported in YQL queries yet`)
    throw new ErrorPerformingSearch({
      message: `${app} is not supported in YQL queries yet`,
      sources: includedApp.join(", "),
    })
  }

  HybridDefaultProfile = (
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    profile: SearchModes = SearchModes.NativeRank,
    timestampRange?: { to: number | null; from: number | null } | null,
    excludedIds?: string[],
    notInMailLabels?: string[],
    excludedApps?: Apps[],
    mailParticipants?: MailParticipant | null,
    userEmail?: string,
    orderBy: "asc" | "desc" = "desc",
    owner?: string | string[] | null,
    attendees?: string[] | null,
    eventStatus?: EventStatusType | null,
    processedCollectionSelections?: CollectionVespaIds | null,
  ): YqlProfile => {
    try {
      const availableSources = this.getAvailableSources(excludedApps)
      if (
        processedCollectionSelections &&
        Object.keys(processedCollectionSelections).length > 0
      ) {
        if (!availableSources.includes(KbItemsSchema)) {
          availableSources.push(KbItemsSchema)
        }
      }
      const includedApps = this.getIncludedApps(excludedApps)

      const appQueries = this.buildAppSpecificQueries(
        includedApps,
        hits,
        app,
        entity,
        timestampRange,
        notInMailLabels,
        mailParticipants,
        owner,
        attendees,
        eventStatus,
      )

      let kbAppQuery: YqlCondition | null = null
      if (
        includedApps.includes(Apps.KnowledgeBase) &&
        processedCollectionSelections
      ) {
        const collectionConds = this.buildCollectionConditions(
          processedCollectionSelections,
        )
        if (collectionConds.length > 0) {
          kbAppQuery = this.buildCollectionFileYQL(hits, collectionConds)
        }
      }

      const yqlBuilder = YqlBuilder.create({
        email: userEmail,
        requirePermissions: true,
        sources: availableSources,
        targetHits: hits,
      })

      yqlBuilder.from(availableSources)
      if (appQueries.length > 0) {
        const combinedCondition = kbAppQuery
          ? Or.withoutPermissions([or(appQueries), kbAppQuery])
          : or(appQueries)
        if (!app && !entity) {
          combinedCondition.and(this.getExcludeAttachmentCondition())
        }
        yqlBuilder.where(combinedCondition)
      }

      if (app !== null && app !== undefined) {
        yqlBuilder.filterByApp(app)
      }

      if (entity !== null && entity !== undefined) {
        yqlBuilder.filterByEntity(entity)
      }

      if (excludedIds && excludedIds.length > 0) {
        yqlBuilder.excludeDocIds(excludedIds)
      }

      return yqlBuilder.buildProfile(profile)
    } catch (error) {
      this.logger.error(`Failed to build YQL profile: ${JSON.stringify(error)}`)
      throw new Error(`Failed to build YQL profile: ${JSON.stringify(error)}`)
    }
  }

  private getAvailableSources(excludedApps?: Apps[]): VespaSchema[] {
    let sources = this.schemaSources

    if (excludedApps && excludedApps.length > 0) {
      const sourcesToExclude: VespaSchema[] = []

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

      sources = sources.filter((source) => !sourcesToExclude.includes(source))
    }

    return sources
  }

  private getIncludedApps(excludedApps?: Apps[]): Apps[] {
    const allApps = Object.values(Apps)
    return allApps.filter((appItem) => !excludedApps?.includes(appItem))
  }

  private buildAppSpecificQueries(
    includedApps: Apps[],
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    notInMailLabels?: string[],
    mailParticipants?: MailParticipant | null,
    owner?: string | string[] | null,
    attendees?: string[] | null,
    eventStatus?: EventStatusType | null,
  ) {
    if (includedApps.length === 0) return []

    const appConditions = []

    if (
      includedApps.includes(Apps.GoogleDrive) ||
      includedApps.includes(Apps.GoogleCalendar)
    ) {
      // this default condition will cover for calendar and drive
      appConditions.push(
        this.buildDefaultCondition(
          hits,
          app,
          entity,
          timestampRange,
          owner,
          attendees,
          eventStatus,
          includedApps,
        ),
      )
    }

    if (includedApps.includes(Apps.GoogleWorkspace)) {
      appConditions.push(
        this.buildGoogleWorkspaceCondition(hits, app, entity, timestampRange),
      )
    }
    if (includedApps.includes(Apps.Gmail)) {
      appConditions.push(
        this.buildGmailCondition(
          hits,
          app,
          entity,
          timestampRange,
          notInMailLabels,
          mailParticipants,
        ),
      )
    }
    if (includedApps.includes(Apps.Slack)) {
      appConditions.push(
        this.buildSlackCondition(hits, app, entity, timestampRange),
      )
    }

    return appConditions
  }

  private buildDataSourceFileYQL = (
    hits: number,
    selectedItem: Record<string, unknown>,
  ) => {
    const dsIds = (selectedItem as Record<string, unknown>)[
      Apps.DataSource
    ] as any
    const dataSourceIdConditions = dsIds.map((id: string) =>
      contains("dataSourceId", id.trim()),
    )
    const searchCondition = Or.withoutPermissions([
      userInput("@query", hits),
      nearestNeighbor("chunk_embeddings", "e", hits),
    ])

    return And.withoutPermissions([searchCondition, dataSourceIdConditions])
  }

  private buildCollectionFileYQL = (
    hits: number,
    conditions: YqlCondition[],
  ) => {
    const searchCondition = Or.withoutPermissions([
      userInput("@query", hits),
      nearestNeighbor("chunk_embeddings", "e", hits),
    ])

    return And.withoutPermissions([
      searchCondition,
      Or.withoutPermissions(conditions),
    ])
  }

  private buildGoogleWorkspaceCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
  ) {
    const permissionBasedConditions = []

    permissionBasedConditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )

    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      permissionBasedConditions.push(
        timestamp("creationTime", "creationTime", timestampRange),
      )
    }

    const hasAppOrEntity = !!(app || entity)
    if (!hasAppOrEntity) {
      permissionBasedConditions.push(contains("app", Apps.GoogleWorkspace))
    }

    const permissionBasedQuery = and(permissionBasedConditions)

    const ownershipBasedConditions = []

    ownershipBasedConditions.push(
      Or.withOwnerPermissions([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )

    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      ownershipBasedConditions.push(
        timestamp("creationTime", "creationTime", timestampRange),
      )
    }

    if (Array.isArray(app) && app.length > 0) {
      const appConditions = app.map((a) => contains("app", a))
      ownershipBasedConditions.push(Or.withOwnerPermissions(appConditions))
    } else if (app && !Array.isArray(app)) {
      ownershipBasedConditions.push(contains("app", app))
    }

    if (Array.isArray(entity) && entity.length > 0) {
      const entityConditions = entity.map((e) => contains("entity", e))
      ownershipBasedConditions.push(Or.withOwnerPermissions(entityConditions))
    } else if (entity && !Array.isArray(entity)) {
      ownershipBasedConditions.push(contains("entity", entity))
    }
    ownershipBasedConditions.push(contains("app", Apps.GoogleWorkspace))
    const ownershipBasedQuery = and(ownershipBasedConditions)

    return Or.withoutPermissions([permissionBasedQuery, ownershipBasedQuery])
  }

  private buildGmailCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    notInMailLabels?: string[],
    mailParticipants?: MailParticipant | null,
  ) {
    const conditions = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )
    conditions.push(contains("app", Apps.Gmail))

    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(timestamp("timestamp", "timestamp", timestampRange))
    }

    if (notInMailLabels && notInMailLabels.length > 0) {
      const labelConditions = notInMailLabels
        .filter((label) => label && label.trim())
        .map((label) => contains("labels", label.trim()))

      if (labelConditions.length > 0) {
        const combinedLabels =
          labelConditions.length === 1
            ? labelConditions[0]!
            : or(labelConditions)

        conditions.push(combinedLabels.not())
      }
    }

    if (mailParticipants && Object.keys(mailParticipants).length > 0) {
      const mailParticipantsCondition = getGmailParticipantsConditions(
        mailParticipants,
        this.logger,
      )

      if (mailParticipantsCondition && mailParticipantsCondition.length > 0) {
        conditions.push(and(mailParticipantsCondition))
      }
    }

    return and(conditions)
  }

  private buildGoogleDriveAgentCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    driveIdsCondition?: YqlCondition,
  ) {
    const conditions = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )
    conditions.push(contains("app", Apps.GoogleDrive))
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(timestamp("updatedAt", "updatedAt", timestampRange))
    }

    if (driveIdsCondition) {
      conditions.push(driveIdsCondition)
    }

    return and(conditions)
  }

  private buildAgentGoogleCalendarCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
  ) {
    const conditions = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )
    conditions.push(contains("app", Apps.GoogleCalendar))
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(timestamp("startTime", "startTime", timestampRange))
    }

    return and(conditions)
  }

  private buildSlackCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    channelIdsCondition?: YqlCondition,
  ): YqlCondition {
    const conditions: YqlCondition[] = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("text_embeddings", "e", hits),
      ]),
    )
    conditions.push(contains("app", Apps.Slack))
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(timestamp("updatedAt", "updatedAt", timestampRange))
    }
    if (channelIdsCondition) {
      conditions.push(channelIdsCondition)
    }

    return conditions.length > 1 ? and(conditions) : conditions[0]!
  }

  // Enhanced Slack condition that supports additional filters like senderId
  private buildEnhancedSlackCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    channelIdsCondition?: YqlCondition,
    senderCondition?: YqlCondition,
  ) {
    const conditions = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("text_embeddings", "e", hits),
      ]),
    )
    conditions.push(contains("app", Apps.Slack))
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(timestamp("updatedAt", "updatedAt", timestampRange))
    }
    if (channelIdsCondition) {
      conditions.push(channelIdsCondition)
    }
    if (senderCondition) {
      conditions.push(senderCondition)
    }

    return and(conditions)
  }

  private buildDefaultCondition(
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    timestampRange?: { to: number | null; from: number | null } | null,
    owner?: string | string[] | null,
    attendees?: string[] | null,
    eventStatus?: EventStatusType | null,
    includedApps?: Apps[],
  ) {
    const conditions = []

    conditions.push(
      or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ]),
    )
    conditions.push(
      or([
        contains("app", Apps.GoogleDrive),
        contains("app", Apps.GoogleCalendar),
      ]),
    )
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      const timestampConds = []
      if (includedApps?.includes(Apps.GoogleCalendar)) {
        timestampConds.push(timestamp("startTime", "startTime", timestampRange))
      }

      if (includedApps?.includes(Apps.GoogleDrive)) {
        timestampConds.push(timestamp("updatedAt", "updatedAt", timestampRange))
      }

      if (timestampConds.length === 1) {
        if (timestampConds[0]) {
          conditions.push(timestampConds[0])
        }
      } else if (timestampConds.length > 1) {
        conditions.push(or(timestampConds))
      }
    }

    if (app && app === Apps.GoogleDrive) {
      if (owner && typeof owner === "string") {
        conditions.push(
          isValidEmail(owner)
            ? contains("owner", owner)
            : matches("owner", owner),
        )
      } else if (owner && Array.isArray(owner) && owner.length > 0) {
        const ownerConditions = owner.map((o) =>
          isValidEmail(o) ? contains("owners", o) : matches("owners", o),
        )
        conditions.push(or(ownerConditions))
      }
    }

    if (app && app === Apps.GoogleCalendar) {
      if (attendees && Array.isArray(attendees) && attendees.length > 0) {
        const attendeeConditions = attendees.map((a) =>
          isValidEmail(a) ? contains("attendees", a) : matches("attendees", a),
        )
        conditions.push(or(attendeeConditions))
      }
      if (eventStatus) {
        conditions.push(contains("status", eventStatus))
      }
    }

    return and(conditions)
  }
  // knowledge base collections conditions will not have permissions checks
  buildCollectionConditions = (
    selections: CollectionVespaIds,
  ): YqlCondition[] => {
    const conds: YqlCondition[] = []
    const { collectionIds, collectionFolderIds, collectionFileIds } = selections

    if (collectionIds?.length) {
      conds.push(
        Or.withoutPermissions(
          collectionIds.map((id) => contains("clId", id.trim())),
        ),
      )
    }
    if (collectionFolderIds?.length) {
      conds.push(
        Or.withoutPermissions(
          collectionFolderIds.map((id) => contains("clFd", id.trim())),
        ),
      )
    }
    if (collectionFileIds?.length) {
      conds.push(
        Or.withoutPermissions(
          collectionFileIds.map((id) => contains("docId", id.trim())),
        ),
      )
    }
    return conds
  }

  HybridDefaultProfileForAgent = (
    hits: number,
    app: Apps | Apps[] | null,
    entity: Entity | Entity[] | null,
    profile: SearchModes = SearchModes.NativeRank,
    timestampRange?: { to: number | null; from: number | null } | null,
    excludedIds?: string[],
    notInMailLabels?: string[],
    allowedApps: Apps[] | null = null,
    dataSourceIds: string[] = [],
    mailParticipants: MailParticipant | null = null,
    channelIds: string[] = [],
    processedCollectionSelections: CollectionVespaIds = {},
    driveIds: string[] = [],
    selectedItem: Record<string, unknown> = {},
    email?: string,
    appFilters: Partial<Record<Apps, AppFilter[]>> = {},
  ): YqlProfile => {
    const appQueries: YqlCondition[] = []
    const sources = new Set<VespaSchema>()

    const buildDocsInclusionCondition = (fieldName: string, ids: string[]) => {
      if (!ids || ids.length === 0) return

      const conditions = ids.map((id) => contains(fieldName, id.trim()))
      return Or.withoutPermissions(conditions)
    }

    // --- App handler dispatcher ---
    const handleApp = (allowedApp: Apps) => {
      switch (allowedApp) {
        case Apps.GoogleWorkspace:
          appQueries.push(
            this.buildGoogleWorkspaceCondition(
              hits,
              app,
              entity,
              timestampRange,
            ),
          )
          sources.add(userSchema)
          break

        case Apps.Gmail:
          // Build enhanced Gmail condition with appFilters - using new direct format
          const gmailFilters = appFilters[Apps.Gmail] || []

          // Handle new multiple filters format
          const gmailFilterConditions: YqlCondition[] = []

          if (
            gmailFilters &&
            Array.isArray(gmailFilters) &&
            gmailFilters.length > 0
          ) {
            // Process multiple filter groups
            for (const filter of gmailFilters) {
              const groupConditions: YqlCondition[] = []

              // Build mail participant conditions for this filter group
              const filterParticipants: MailParticipant = {}
              if (filter.from && Array.isArray(filter.from)) {
                filterParticipants.from = filter.from.filter(Boolean)
              }
              if (filter.to && Array.isArray(filter.to)) {
                filterParticipants.to = filter.to.filter(Boolean)
              }
              if (filter.cc && Array.isArray(filter.cc)) {
                filterParticipants.cc = filter.cc.filter(Boolean)
              }
              if (filter.bcc && Array.isArray(filter.bcc)) {
                filterParticipants.bcc = filter.bcc.filter(Boolean)
              }

              // Merge filter participants with existing mailParticipants
              const enhancedParticipants =
                this.buildIntersectionMailParticipants(mailParticipants, {
                  ...filterParticipants,
                })

              // Add participant conditions for this filter group
              if (
                enhancedParticipants &&
                Object.keys(enhancedParticipants).length > 0
              ) {
                const participantConditions = getGmailParticipantsConditions(
                  enhancedParticipants,
                  this.logger,
                )
                if (participantConditions.length > 0) {
                  groupConditions.push(...participantConditions)
                }
              }

              // Add time range condition for this filter group (merge with existing timestamp range)
              if (filter.timeRange) {
                const mergedTimestampRange = this.mergeTimestampRanges(
                  timestampRange,
                  filter.timeRange,
                  Apps.Gmail,
                )
                if (mergedTimestampRange) {
                  const timeCondition = timestamp(
                    "timestamp",
                    "timestamp",
                    mergedTimestampRange,
                  )
                  groupConditions.push(timeCondition)
                }
              } else if (timestampRange) {
                // Use global timestamp range if no filter-specific range
                const timeCondition = timestamp(
                  "timestamp",
                  "timestamp",
                  timestampRange,
                )
                groupConditions.push(timeCondition)
              }

              // Add not in mail labels condition
              if (notInMailLabels && notInMailLabels.length > 0) {
                const labelConditions = notInMailLabels
                  .filter((label) => label && label.trim())
                  .map((label) => contains("labels", label.trim()))

                if (labelConditions.length > 0) {
                  const combinedLabels =
                    labelConditions.length === 1
                      ? labelConditions[0]!
                      : or(labelConditions)
                  groupConditions.push(combinedLabels.not())
                }
              }

              // If this filter group has conditions, add them as an AND group
              if (groupConditions.length > 0) {
                gmailFilterConditions.push(and(groupConditions))
              }
            }
          }

          // Build final Gmail condition - either enhanced or standard
          if (gmailFilterConditions.length > 0) {
            const baseConditions = [
              or([
                userInput("@query", hits),
                nearestNeighbor("chunk_embeddings", "e", hits),
              ]),
            ]

            // Add combined filter conditions (OR between multiple filter groups)
            baseConditions.push(or(gmailFilterConditions))
            baseConditions.push(or([contains("app", Apps.Gmail)]))

            appQueries.push(and(baseConditions))
          } else {
            // No valid filter groups, use standard condition
            appQueries.push(
              this.buildGmailCondition(
                hits,
                app,
                entity,
                timestampRange,
                notInMailLabels,
                mailParticipants,
              ),
            )
          }

          sources.add(mailSchema)
          break
        case Apps.GoogleDrive:
          const driveIdsCond = buildDocsInclusionCondition("docId", driveIds)
          appQueries.push(
            this.buildGoogleDriveAgentCondition(
              hits,
              app,
              entity,
              timestampRange,
              driveIdsCond,
            ),
          )
          sources.add(fileSchema)
          break

        case Apps.GoogleCalendar:
          appQueries.push(
            this.buildAgentGoogleCalendarCondition(
              hits,
              app,
              entity,
              timestampRange,
            ),
          )
          sources.add(eventSchema)
          break

        case Apps.Slack:
          // Build enhanced Slack condition with appFilters - using new direct format
          const slackFilters = appFilters[Apps.Slack] || []
          const slackChannelIds =
            (selectedItem[Apps.Slack] as string[]) || channelIds
          const channelCond = buildDocsInclusionCondition(
            "channelId",
            slackChannelIds,
          )

          // Handle new multiple filters format
          const slackFilterConditions: YqlCondition[] = []
          if (
            slackFilters &&
            Array.isArray(slackFilters) &&
            slackFilters.length > 0
          ) {
            // Process multiple filter groups
            for (const filter of slackFilters) {
              const groupConditions: YqlCondition[] = []

              // Add senderId conditions for this filter group
              if (
                filter.senderId &&
                Array.isArray(filter.senderId) &&
                filter.senderId.length > 0
              ) {
                const senderConditions = filter.senderId.map(
                  (senderId: string) => contains("userId", senderId.trim()),
                )
                groupConditions.push(or(senderConditions))
              }

              // Add channelId conditions for this filter group
              if (
                filter.channelId &&
                Array.isArray(filter.channelId) &&
                filter.channelId.length > 0
              ) {
                const channelConditions = filter.channelId.map(
                  (channelId: string) =>
                    contains("channelId", channelId.trim()),
                )
                groupConditions.push(or(channelConditions))
              }

              // Add time range condition for this filter group (merge with existing timestamp range)
              if (filter.timeRange) {
                const mergedTimestampRange = this.mergeTimestampRanges(
                  timestampRange,
                  filter.timeRange,
                  Apps.Slack,
                )
                if (mergedTimestampRange) {
                  const timeCondition = timestamp(
                    "updatedAt",
                    "updatedAt",
                    mergedTimestampRange,
                  )
                  groupConditions.push(timeCondition)
                }
              } else if (timestampRange) {
                // Use global timestamp range if no filter-specific range
                const timeCondition = timestamp(
                  "updatedAt",
                  "updatedAt",
                  timestampRange,
                )
                groupConditions.push(timeCondition)
              }

              // If this filter group has conditions, add them as an AND group
              if (groupConditions.length > 0) {
                slackFilterConditions.push(and(groupConditions))
              }
            }
          }

          // Build final Slack condition - either enhanced or standard
          if (slackFilterConditions.length > 0) {
            const baseConditions = [
              or([
                userInput("@query", hits),
                nearestNeighbor("text_embeddings", "e", hits),
              ]),
            ]

            // Add the channel condition from selectedItem if present
            if (channelCond) {
              baseConditions.push(channelCond)
            }

            // Add combined filter conditions (OR between multiple filter groups)
            baseConditions.push(or(slackFilterConditions))
            baseConditions.push(or([contains("app", Apps.Slack)]))

            appQueries.push(and(baseConditions))
          } else {
            // No valid filter groups, use standard enhanced condition
            appQueries.push(
              this.buildEnhancedSlackCondition(
                hits,
                app,
                entity,
                timestampRange,
                channelCond,
                undefined,
              ),
            )
          }

          sources
            .add(chatUserSchema)
            .add(chatMessageSchema)
            .add(chatContainerSchema)
          break
        case Apps.DataSource:
          appQueries.push(this.buildDataSourceFileYQL(hits, selectedItem))
          sources.add(dataSourceFileSchema)
          break

        case Apps.KnowledgeBase:
          const collectionConds = this.buildCollectionConditions(
            processedCollectionSelections,
          )
          if (collectionConds.length > 0) {
            appQueries.push(this.buildCollectionFileYQL(hits, collectionConds))
            sources.add(KbItemsSchema)
          } else {
            this.logger.warn(
              "KnowledgeBase specified but no valid selections found. Skipping.",
            )
          }
          break
      }
    }

    if (allowedApps?.length) {
      allowedApps.forEach(handleApp)
    } else if (dataSourceIds?.length) {
      // fallback: only data sources
      appQueries.push(this.buildDataSourceFileYQL(hits, selectedItem))
      sources.add(dataSourceFileSchema)
    }

    const yqlBuilder = YqlBuilder.create({
      email,
      requirePermissions: true,
      sources: [...sources],
      targetHits: hits,
    })

    yqlBuilder.from([...sources])
    if (appQueries.length > 0) {
      // Add queries without permission checks to support knowledge base collections
      if (!app && !entity) {
        yqlBuilder.where(
          And.withoutPermissions([
            Or.withoutPermissions(appQueries),
            this.getExcludeAttachmentCondition(),
          ]),
        )
      } else {
        yqlBuilder.where(Or.withoutPermissions(appQueries))
      }
    }

    if (app) yqlBuilder.filterByApp(app)
    if (entity) yqlBuilder.filterByEntity(entity)
    if (excludedIds?.length) yqlBuilder.excludeDocIds(excludedIds)

    return yqlBuilder.buildProfile(profile)
  }

  HybridDefaultProfileInFiles = (
    hits: number,
    profile: SearchModes = SearchModes.NativeRank,
    fileIds: string[],
    notInMailLabels?: string[],
    email?: string,
  ): YqlProfile => {
    const buildContextFilters = (ids: string[]): YqlCondition[] =>
      ids.filter(Boolean).map((id) => contains("docId", id.trim()))

    const buildDocOrMailSearch = (): YqlCondition => {
      const baseSearch = or([
        userInput("@query", hits),
        nearestNeighbor("chunk_embeddings", "e", hits),
      ])

      if (!notInMailLabels?.length) return baseSearch

      const labelFilters = notInMailLabels
        .filter((label) => label && label.trim())
        .map((label) => contains("labels", label.trim()))

      return and([baseSearch, or(labelFilters).not()])
    }

    const buildSlackSearch = (): YqlCondition =>
      or([
        userInput("@query", hits),
        nearestNeighbor("text_embeddings", "e", hits),
      ])

    const buildContactSearch = (): YqlCondition => userInput("@query", hits)

    // --- search conditions ---
    let searchConditions: YqlCondition[] = [
      buildDocOrMailSearch(),
      buildSlackSearch(),
      buildContactSearch(),
    ]

    const contextFilters = buildContextFilters(fileIds)
    if (contextFilters.length > 0) {
      searchConditions = [or(searchConditions), or(contextFilters)]
    }

    const sources: VespaSchema[] =
      profile === SearchModes.AttachmentRank ? [fileSchema] : this.schemaSources

    return YqlBuilder.create({
      email,
      requirePermissions: true,
      sources,
      targetHits: hits,
    })
      .from(sources)
      .where(and(searchConditions))
      .buildProfile(profile)
  }

  HybridDefaultProfileForSlack = (
    hits: number,
    profile: SearchModes = SearchModes.NativeRank,
    channelIds?: string[],
    threadId?: string,
    userId?: string,
    timestampRange?: { to: number | null; from: number | null } | null,
    email?: string,
  ): YqlProfile => {
    let conditions: YqlCondition[] = [
      or([
        userInput("@query", hits),
        nearestNeighbor("text_embeddings", "e", hits),
      ]),
    ]

    if (timestampRange && !timestampRange.from && !timestampRange.to) {
      conditions.push(timestamp("createdAt", "createdAt", timestampRange))
    }

    if (channelIds && channelIds.length > 0) {
      conditions.push(or(channelIds.map((id) => contains("channelId", id))))
    }
    if (threadId) {
      conditions.push(contains("threadId", threadId))
    }
    if (userId) {
      conditions.push(contains("userId", userId))
    }

    return YqlBuilder.create({
      email,
      requirePermissions: true,
      sources: [chatMessageSchema],
      targetHits: hits,
    })
      .from([chatMessageSchema])
      .where(and(conditions))
      .buildProfile(profile)
  }

  HybridDefaultProfileAppEntityCounts = (
    hits: number,
    timestampRange: { to: number; from: number } | null,
    notInMailLabels?: string[],
    excludedApps?: Apps[],
    email?: string,
  ): YqlProfile => {
    const includedApps = this.getIncludedApps(excludedApps)
    let conditions: YqlCondition[] = []

    if (
      includedApps.includes(Apps.GoogleDrive) ||
      includedApps.includes(Apps.GoogleCalendar)
    ) {
      conditions.push(
        this.buildDefaultCondition(
          hits,
          null,
          null,
          timestampRange,
          null,
          null,
          null,
          includedApps,
        ),
      )
    }

    if (includedApps.includes(Apps.Gmail)) {
      conditions.push(
        this.buildGmailCondition(
          hits,
          null,
          null,
          timestampRange,
          notInMailLabels,
        ),
      )
    }

    if (includedApps.includes(Apps.Slack)) {
      conditions.push(
        this.buildSlackCondition(hits, null, null, timestampRange),
      )
    }

    if (includedApps.includes(Apps.GoogleWorkspace)) {
      conditions.push(
        this.buildGoogleWorkspaceCondition(hits, null, null, timestampRange),
      )
    }

    // Start with AllSources and filter out excluded app schemas
    let newSources = this.getSchemaSources()
    if (excludedApps?.length) {
      const sourcesToExclude = excludedApps.reduce<string[]>(
        (acc, excludedApp) => {
          switch (excludedApp) {
            case Apps.Slack:
              return acc.concat([chatMessageSchema, chatUserSchema])
            case Apps.Gmail:
              return acc.concat([mailSchema, mailAttachmentSchema])
            case Apps.GoogleDrive:
              return acc.concat([fileSchema])
            case Apps.GoogleCalendar:
              return acc.concat([eventSchema])
            case Apps.GoogleWorkspace:
              return acc.concat([userSchema])
            default:
              return acc
          }
        },
        [],
      )

      newSources = this.getSchemaSources().filter(
        (source) => !sourcesToExclude.includes(source),
      )
    }

    return YqlBuilder.create({
      email,
      requirePermissions: true,
      sources: newSources,
      targetHits: hits,
    })
      .from(newSources)
      .where(and([or(conditions), this.getExcludeAttachmentCondition()]))
      .limit(0)
      .groupBy(`
        all(
              group(app) each(
                  group(entity) each(output(count()))
              )
            )
        `)
      .buildProfile(SearchModes.NativeRank)
  }

  private filterAttachmentApp = (
    app: Apps | Apps[] | null | undefined,
  ): Apps | Apps[] | null => {
    if (!app) {
      return null
    }

    if (Array.isArray(app)) {
      const filteredApps = app.filter((a) => a !== Apps.Attachment)
      return filteredApps.length === 0 ? null : filteredApps
    }

    return app === Apps.Attachment ? null : app
  }

  private filterAttachmentEntity = (
    entity: Entity | Entity[] | null | undefined,
  ): Entity | Entity[] | null => {
    if (!entity) {
      return null
    }

    if (Array.isArray(entity)) {
      const filteredEntities = entity.filter(
        (e) => !Object.values(AttachmentEntity).includes(e as AttachmentEntity),
      )
      return filteredEntities.length === 0 ? null : filteredEntities
    }

    return Object.values(AttachmentEntity).includes(entity as AttachmentEntity)
      ? null
      : entity
  }

  private getExcludeAttachmentCondition = (): YqlCondition => {
    const appCondition = contains("app", Apps.Attachment)
    return not(appCondition)
  }

  // Helper function to build enhanced mail participants from individual filter
  // Uses intersection logic: if both existing and filter participants exist, take intersection
  // If only one exists, use that one
  private buildIntersectionMailParticipants = (
    existingParticipants: MailParticipant | null,
    filterParticipants: MailParticipant,
  ): MailParticipant | null => {
    // If no filter participants, return existing participants
    if (!filterParticipants || Object.keys(filterParticipants).length === 0) {
      return existingParticipants
    }

    // If no existing participants, use only filter participants
    if (
      !existingParticipants ||
      Object.keys(existingParticipants).length === 0
    ) {
      return Object.keys(filterParticipants).length === 0
        ? null
        : filterParticipants
    }

    // Both exist - take intersection
    const intersectedParticipants: MailParticipant = {}

    if (existingParticipants.from && filterParticipants.from) {
      intersectedParticipants.from = existingParticipants.from.filter((email) =>
        filterParticipants.from!.includes(email),
      )
    } else if (existingParticipants.from) {
      intersectedParticipants.from = existingParticipants.from
    } else if (filterParticipants.from) {
      intersectedParticipants.from = filterParticipants.from
    }

    if (existingParticipants.to && filterParticipants.to) {
      intersectedParticipants.to = existingParticipants.to.filter((email) =>
        filterParticipants.to!.includes(email),
      )
    } else if (existingParticipants.to) {
      intersectedParticipants.to = existingParticipants.to
    } else if (filterParticipants.to) {
      intersectedParticipants.to = filterParticipants.to
    }

    if (existingParticipants.cc && filterParticipants.cc) {
      intersectedParticipants.cc = existingParticipants.cc.filter((email) =>
        filterParticipants.cc!.includes(email),
      )
    } else if (existingParticipants.cc) {
      intersectedParticipants.cc = existingParticipants.cc
    } else if (filterParticipants.cc) {
      intersectedParticipants.cc = filterParticipants.cc
    }

    if (existingParticipants.bcc && filterParticipants.bcc) {
      intersectedParticipants.bcc = existingParticipants.bcc.filter((email) =>
        filterParticipants.bcc!.includes(email),
      )
    } else if (existingParticipants.bcc) {
      intersectedParticipants.bcc = existingParticipants.bcc
    } else if (filterParticipants.bcc) {
      intersectedParticipants.bcc = filterParticipants.bcc
    }

    return Object.keys(intersectedParticipants).length === 0
      ? null
      : intersectedParticipants
  }

  // Helper function to merge timestamp ranges
  // Uses intersection logic: if both existing and app filters exist, take intersection
  // If only one exists, use that one
  private mergeTimestampRanges = (
    existingRange:
      | { to: number | null; from: number | null }
      | null
      | undefined,
    filterTimeRange: { startDate: number; endDate: number } | undefined,
    app?: Apps,
  ): { to: number | null; from: number | null } | null => {
    this.logger.info(JSON.stringify(existingRange))
    this.logger.info(JSON.stringify(filterTimeRange))

    // Helper function to normalize timestamp to milliseconds (13 digits)

    // If no app filter time range, return existing range (normalized)
    if (!filterTimeRange) {
      if (!existingRange) return null

      return {
        to: existingRange.to ? normalizeTimestamp(existingRange.to, app) : null,
        from: existingRange.from
          ? normalizeTimestamp(existingRange.from, app)
          : null,
      }
    }

    // Extract and normalize app filter time range
    const appFilterRange: { to: number | null; from: number | null } = {
      to: filterTimeRange.endDate
        ? normalizeTimestamp(filterTimeRange.endDate, app)
        : null,
      from: filterTimeRange.startDate
        ? normalizeTimestamp(filterTimeRange.startDate, app)
        : null,
    }

    // If no existing range, use only app filter range (already normalized)
    if (
      !existingRange ||
      (existingRange.to === null && existingRange.from === null)
    ) {
      return appFilterRange
    }

    // Normalize existing range
    const normalizedExistingRange = {
      to: existingRange.to ? normalizeTimestamp(existingRange.to, app) : null,
      from: existingRange.from
        ? normalizeTimestamp(existingRange.from, app)
        : null,
    }

    // Both exist - take intersection (most restrictive range)
    const merged: { to: number | null; from: number | null } = {
      from: null,
      to: null,
    }

    // For from (start): take the later/higher timestamp (more restrictive start)
    if (normalizedExistingRange.from !== null && appFilterRange.from !== null) {
      merged.from = Math.max(normalizedExistingRange.from, appFilterRange.from)
    } else if (normalizedExistingRange.from !== null) {
      merged.from = normalizedExistingRange.from
    } else if (appFilterRange.from !== null) {
      merged.from = appFilterRange.from
    }

    // For to (end): take the earlier/lower timestamp (more restrictive end)
    if (normalizedExistingRange.to !== null && appFilterRange.to !== null) {
      merged.to = Math.min(normalizedExistingRange.to, appFilterRange.to)
    } else if (normalizedExistingRange.to !== null) {
      merged.to = normalizedExistingRange.to
    } else if (appFilterRange.to !== null) {
      merged.to = appFilterRange.to
    }

    return merged
  }

  getAllDocumentsForAgent = async (
    AllowedApps: Apps[] | null,
    dataSourceIds: string[] = [],
    limit: number = 400,
    email: string,
  ): Promise<VespaSearchResponse | null> => {
    const sources: VespaSchema[] = []
    const conditions: YqlCondition[] = []

    if (AllowedApps && AllowedApps.length > 0) {
      for (const allowedApp of AllowedApps) {
        switch (allowedApp) {
          case Apps.GoogleWorkspace:
            if (!sources.includes(userSchema)) sources.push(userSchema)
            conditions.push(contains("app", Apps.GoogleWorkspace))
            break
          case Apps.Gmail:
            if (!sources.includes(mailSchema)) sources.push(mailSchema)
            conditions.push(contains("app", Apps.Gmail))
            break
          case Apps.GoogleDrive:
            if (!sources.includes(fileSchema)) sources.push(fileSchema)
            conditions.push(contains("app", Apps.GoogleDrive))
            break
          case Apps.GoogleCalendar:
            if (!sources.includes(eventSchema)) sources.push(eventSchema)
            conditions.push(contains("app", Apps.GoogleCalendar))
            break
          case Apps.Slack:
            if (!sources.includes(chatUserSchema)) sources.push(chatUserSchema)
            if (!sources.includes(chatMessageSchema))
              sources.push(chatMessageSchema)
            conditions.push(contains("app", Apps.Slack))
            break
          case Apps.DataSource:
            if (dataSourceIds && dataSourceIds.length > 0) {
              if (!sources.includes(dataSourceFileSchema))
                sources.push(dataSourceFileSchema)
              const dsConditions = dataSourceIds.map((id) =>
                contains("dataSourceId", id.trim()),
              )
              conditions.push(Or.withoutPermissions(dsConditions))
            }
            break
        }
      }
    } else if (dataSourceIds && dataSourceIds.length > 0) {
      if (!sources.includes(dataSourceFileSchema))
        sources.push(dataSourceFileSchema)
      const dsConditions = dataSourceIds.map((id) =>
        contains("dataSourceId", id.trim()),
      )
      conditions.push(Or.withoutPermissions(dsConditions))
    }

    const schemaSources = [...new Set(sources)]
    const yql = YqlBuilder.create({
      email,
      requirePermissions: true,
      sources: schemaSources,
      targetHits: limit,
    })
      .from(schemaSources)
      .where(and([or(conditions), this.getExcludeAttachmentCondition()]))
      .build()

    const payload = {
      yql,
      hits: limit,
      timeout: "30s",
      "ranking.profile": "unranked",
    }

    return this.vespa.search<VespaSearchResponse>(payload).catch((error) => {
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: schemaSources.join(", "),
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

    let { yql, profile } = this.HybridDefaultProfileAppEntityCounts(
      limit,
      timestampRange ?? null,
      [], // notInMailLabels
      excludedApps,
      email,
    )
    // console.log("Vespa YQL Query in group vespa: ", formatYqlToReadable(yql))
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
      this.logger.error("Error in group vespa search: ", error)
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources: this.getSchemaSourcesString(),
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
      mailParticipants = {},
      isSlackConnected,
      isCalendarConnected,
      isDriveConnected,
      isGmailConnected,
      orderBy = "desc",
      owner = null,
      attendees = null,
      eventStatus = null,
      processedCollectionSelections = {},
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    // Filter out attachment app and entities if present
    const filteredApp = this.filterAttachmentApp(app)
    const filteredEntity = this.filterAttachmentEntity(entity)

    // either no prod config, or prod call errored
    return await this._searchVespa(query, email, filteredApp, filteredEntity, {
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
      mailParticipants,
      isSlackConnected,
      isCalendarConnected,
      isDriveConnected,
      isGmailConnected,
      orderBy,
      owner,
      attendees,
      eventStatus,
      processedCollectionSelections,
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
      mailParticipants = {},
      isSlackConnected = false,
      isCalendarConnected = false,
      isDriveConnected = false,
      isGmailConnected = false,
      orderBy = "desc",
      owner = null,
      attendees = null,
      eventStatus = null,
      processedCollectionSelections,
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> {
    // Determine the timestamp cutoff based on lastUpdated
    // const timestamp = lastUpdated ? getTimestamp(lastUpdated) : null
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    // Check if Slack sync job exists for the user (only for local vespa)
    let excludedApps: Apps[] = []
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

    let { yql, profile } = this.HybridDefaultProfile(
      limit,
      app,
      entity,
      rankProfile,
      timestampRange,
      excludedIds,
      notInMailLabels,
      excludedApps,
      mailParticipants,
      email,
      orderBy,
      owner,
      attendees,
      eventStatus,
      processedCollectionSelections,
    )
    // console.log("Vespa YQL Query in search vespa: ", formatYqlToReadable(yql))
    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      "input.query(slackBoost)": 4.0,
      "input.query(fileBoost)": 4.0,
      "input.query(mailBoost)": 4.0,
      "input.query(eventBoost)": 4.0,
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
        sources: this.getSchemaSourcesString(),
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
      email,
    )

    // console.log("Vespa YQL Query: in files ", formatYqlToReadable(yql))
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
          sources: this.getSchemaSourcesString(),
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
      email,
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
      mailParticipants = null,
      channelIds = [],
      driveIds = [], // docIds
      selectedItem,
      processedCollectionSelections = {},
      appFilters = {}, // Add appFilters parameter
    }: Partial<VespaQueryConfig>,
  ): Promise<VespaSearchResponse> => {
    // Determine the timestamp cutoff based on lastUpdated
    // const timestamp = lastUpdated ? getTimestamp(lastUpdated) : null
    const isDebugMode = this.config.isDebugMode || requestDebug || false

    // Filter out attachment app and entities if present
    app = this.filterAttachmentApp(app)
    entity = this.filterAttachmentEntity(entity)
    Apps = this.filterAttachmentApp(Apps) as Apps[] | null

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
      mailParticipants,
      channelIds,
      processedCollectionSelections, // Pass processedCollectionSelections
      driveIds,
      selectedItem,
      email,
      appFilters, // Pass appFilters to the profile builder
    )

    // console.log("Vespa YQL Query: for agent ", formatYqlToReadable(yql))
    const hybridDefaultPayload = {
      yql,
      query,
      email: email,
      "ranking.profile": profile,
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": alpha,
      "input.query(slackBoost)": 4.0,
      "input.query(fileBoost)": 4.0,
      "input.query(mailBoost)": 4.0,
      "input.query(eventBoost)": 4.0,
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
          sources: this.schemaSources.join(", "),
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
    const yqlIds = docIds.map((id) => contains("docId", id))
    const yqlMailIds = docIds.map((id) => contains("mailId", id))

    // Permission checks are skipped here since we're only retrieving documents by their docIds
    const yqlQuery = YqlBuilder.create({ requirePermissions: false })
      .from("*")
      .whereOr(...yqlIds, ...yqlMailIds)
      .build()

    return this.vespa
      .getDocumentsByOnlyDocIds({ ...opts, yql: yqlQuery })
      .catch((error) => {
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
    schema: VespaSchema,
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
      return fuzzy("name", name)
      // For exact match, use:
      // return `(name contains "${name}")`;
    })

    const emailConditions = mentionedEmails.map((email) => {
      // For fuzzy search
      return fuzzy("email", email)
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

  getItems = async (
    params: GetItemsParams & {
      appFilters?: Partial<Record<Apps, AppFilter[]>>
    },
  ): Promise<VespaSearchResponse> => {
    const {
      schema,
      timestampRange,
      limit = this.config.page,
      offset = 0,
      email,
      excludedIds, // Added excludedIds here
      asc,
      mailParticipants,
      channelIds,
      processedCollectionSelections,
      selectedItem,
      driveIds,
      appFilters = {},
    } = params

    let { app, entity } = params

    const schemas = Array.isArray(schema) ? schema : [schema]

    const includesApp = (targetApp: Apps): boolean => {
      if (!app) return false
      if (Array.isArray(app)) {
        return app.includes(targetApp)
      }
      return app === targetApp
    }

    // Construct conditions based on parameters
    let conditions: YqlCondition[] = []
    const slackChannelIds = selectedItem
      ? selectedItem[Apps.Slack]
        ? selectedItem[Apps.Slack]
        : channelIds || []
      : []

    // NEW: Enhanced multiple filter logic for Gmail
    if (
      includesApp(Apps.Gmail) &&
      appFilters[Apps.Gmail] &&
      Array.isArray(appFilters[Apps.Gmail])
    ) {
      const gmailFilters = appFilters[Apps.Gmail]
      const gmailFilterConditions: YqlCondition[] = []

      // Process multiple filter groups
      for (const filter of gmailFilters) {
        const groupConditions: YqlCondition[] = []

        // Build mail participant conditions for this filter group
        const filterParticipants: MailParticipant = {}
        if (filter.from && Array.isArray(filter.from)) {
          filterParticipants.from = filter.from.filter(Boolean)
        }
        if (filter.to && Array.isArray(filter.to)) {
          filterParticipants.to = filter.to.filter(Boolean)
        }
        if (filter.cc && Array.isArray(filter.cc)) {
          filterParticipants.cc = filter.cc.filter(Boolean)
        }
        if (filter.bcc && Array.isArray(filter.bcc)) {
          filterParticipants.bcc = filter.bcc.filter(Boolean)
        }

        // Merge filter participants with existing mailParticipants
        const enhancedParticipants = this.buildIntersectionMailParticipants(
          mailParticipants || null,
          { ...filterParticipants },
        )

        // Add participant conditions for this filter group
        if (
          enhancedParticipants &&
          Object.keys(enhancedParticipants).length > 0
        ) {
          const participantConditions = getGmailParticipantsConditions(
            enhancedParticipants,
            this.logger,
          )
          if (participantConditions.length > 0) {
            groupConditions.push(...participantConditions)
          }
        }

        // Add time range condition for this filter group (merge with existing timestamp range)
        if (filter.timeRange) {
          const mergedTimestampRange = this.mergeTimestampRanges(
            timestampRange,
            filter.timeRange,
            Apps.Gmail,
          )
          if (mergedTimestampRange) {
            const timeCondition = timestamp(
              "timestamp",
              "timestamp",
              mergedTimestampRange,
            )
            groupConditions.push(timeCondition)
          }
        }

        // If this filter group has conditions, add them as an AND group
        if (groupConditions.length > 0) {
          gmailFilterConditions.push(and(groupConditions))
        }
      }

      // Add combined filter conditions (OR between multiple filter groups)
      if (gmailFilterConditions.length > 0) {
        conditions.push(or(gmailFilterConditions))
      }
    }

    // NEW: Enhanced multiple filter logic for Slack
    if (
      includesApp(Apps.Slack) &&
      appFilters[Apps.Slack] &&
      Array.isArray(appFilters[Apps.Slack])
    ) {
      const slackFilters = appFilters[Apps.Slack]
      const slackFilterConditions: YqlCondition[] = []

      for (const filter of slackFilters) {
        const groupConditions: YqlCondition[] = []

        // Add senderId conditions for this filter group
        if (
          filter.senderId &&
          Array.isArray(filter.senderId) &&
          filter.senderId.length > 0
        ) {
          const senderConditions = filter.senderId.map((senderId: string) =>
            contains("userId", senderId.trim()),
          )
          groupConditions.push(or(senderConditions))
        }

        // Add channelId conditions for this filter group
        if (
          filter.channelId &&
          Array.isArray(filter.channelId) &&
          filter.channelId.length > 0
        ) {
          const channelConditions = filter.channelId.map((channelId: string) =>
            contains("channelId", channelId.trim()),
          )
          groupConditions.push(or(channelConditions))
        }

        // Add time range condition for this filter group (merge with existing timestamp range)
        if (filter.timeRange) {
          const mergedTimestampRange = this.mergeTimestampRanges(
            timestampRange,
            filter.timeRange,
            Apps.Slack,
          )
          if (mergedTimestampRange) {
            const timeCondition = timestamp(
              "updatedAt",
              "updatedAt",
              mergedTimestampRange,
            )
            groupConditions.push(timeCondition)
          }
        }

        // If this filter group has conditions, add them as an AND group
        if (groupConditions.length > 0) {
          slackFilterConditions.push(and(groupConditions))
        }
      }

      if (slackFilterConditions.length > 0) {
        conditions.push(or(slackFilterConditions))
      }
    }

    // Standard Slack channel filtering (backward compatibility)
    if (
      includesApp(Apps.Slack) &&
      slackChannelIds &&
      slackChannelIds.length > 0 &&
      !appFilters[Apps.Slack] // Only apply if no enhanced filters
    ) {
      if (!schemas.includes(chatMessageSchema)) schemas.push(chatMessageSchema)
      const channelIdConditions = slackChannelIds.map((id) =>
        contains("channelId", id.trim()),
      )
      conditions.push(or(channelIdConditions))
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
      // Filter out attachment app and entities if present
      app = this.filterAttachmentApp(app)
      entity = this.filterAttachmentEntity(entity)
      timestampField.push("updatedAt")
    } else if (schemas.includes(eventSchema)) {
      timestampField.push("startTime")
    } else if (schemas.includes(userSchema)) {
      timestampField.push("creationTime")
    } else {
      timestampField.push("updatedAt")
    }

    // Timestamp conditions
    if (isValidTimestampRange(timestampRange)) {
      const timeConditions: YqlCondition[] = []
      const validFields = timestampField.filter(Boolean)

      if (validFields.length > 0) {
        timeConditions.push(
          or(
            validFields.map((field) => timestamp(field, field, timestampRange)),
          ),
        )
        conditions.push(...timeConditions)
      }
    }

    const mailParticipantConditions = []
    // Intent-based conditions - modular approach for different apps
    if (mailParticipants) {
      this.logger.debug("Processing mailParticipants-based filtering", {
        mailParticipants,
        app,
        entity,
        schema,
      })

      // Handle Gmail intent filtering
      if (
        includesApp(Apps.Gmail) &&
        entity === MailEntity.Email &&
        schema === mailSchema
      ) {
        const gmailIntentConditions = getGmailParticipantsConditions(
          mailParticipants,
          this.logger,
        )
        if (gmailIntentConditions.length > 0) {
          mailParticipantConditions.push(...gmailIntentConditions)
          this.logger.debug(
            `Added Gmail intent conditions: ${gmailIntentConditions.join(" and ")}`,
          )
        } else {
          this.logger.debug(
            "Gmail participants provided but contains only names/non-specific identifiers - skipping participant filtering",
            { mailParticipants },
          )
        }
      }
    }

    if (driveIds && driveIds.length > 0 && includesApp(Apps.GoogleDrive)) {
      const driveIdConditions = driveIds.map((id) =>
        contains("docId", id.trim()),
      )
      conditions.push(or(driveIdConditions))
    }

    let kbConditions: YqlCondition[] = []
    if (includesApp(Apps.KnowledgeBase) && processedCollectionSelections) {
      kbConditions = this.buildCollectionConditions(
        processedCollectionSelections,
      )
    }

    let appCondition: YqlCondition | undefined
    if (app) {
      appCondition = Array.isArray(app)
        ? or(app.map((a) => contains("app", a)))
        : contains("app", app)
    }

    let entityCondition: YqlCondition | undefined
    if (entity) {
      entityCondition = Array.isArray(entity)
        ? or(entity.map((e) => contains("entity", e)))
        : contains("entity", entity)
    }

    const yqlBuilder = YqlBuilder.create({
      email,
      requirePermissions: true,
    }).from(schema)

    if (!appCondition && !entityCondition) {
      const whereConditions: YqlCondition[] = []

      if (conditions.length > 0) {
        whereConditions.push(and(conditions))
      }

      whereConditions.push(this.getExcludeAttachmentCondition())

      if (kbConditions.length > 0) {
        whereConditions.push(Or.withoutPermissions(kbConditions))
      }

      yqlBuilder.where(
        kbConditions.length > 0
          ? Or.withoutPermissions(whereConditions)
          : and(whereConditions),
      )
    } else {
      const appEntityConditions: YqlCondition[] = []
      if (appCondition) appEntityConditions.push(appCondition)
      if (entityCondition) appEntityConditions.push(entityCondition)

      const mainConditions: YqlCondition[] = []

      if (conditions.length > 0 && appEntityConditions.length > 0) {
        mainConditions.push(and([...conditions, ...appEntityConditions]))
      } else if (conditions.length > 0) {
        mainConditions.push(and(conditions))
      } else if (appEntityConditions.length > 0) {
        mainConditions.push(and(appEntityConditions))
      }

      if (kbConditions.length > 0) {
        mainConditions.push(Or.withoutPermissions(kbConditions))
      }

      yqlBuilder.where(
        kbConditions.length > 0
          ? Or.withoutPermissions(mainConditions)
          : and(mainConditions),
      )
    }

    if (timestampField.length > 0 && timestampField[0]) {
      yqlBuilder.orderBy(timestampField[0], asc ? "asc" : "desc")
    }

    if (excludedIds && excludedIds.length > 0) {
      yqlBuilder.excludeDocIds(excludedIds)
    }

    const yql = yqlBuilder.offset(offset ?? 0).build()
    console.log("Vespa YQL Query in getItems: ", formatYqlToReadable(yql))
    this.logger.info(`[getItems] Query Details:`, {
      schema,
      app,
      entity,
      limit,
      offset,
      mailParticipantsProvided: !!mailParticipants,
      conditions: conditions.length > 0 ? conditions : "none",
    })

    const searchPayload = {
      yql,
      hits: limit,
      ...(offset ? { offset } : {}),
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
    // Fetch a single record matching the given name and creator email without permission checks
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(datasourceSchema)
      .where(
        and([contains("name", name), contains("createdBy", createdByEmail)]),
      )
      .limit(1)
      .build()

    this.logger.info(
      `Fetching DataSource by name "${name}" and creator "${createdByEmail}"`,
    )
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
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(datasourceSchema)
      .where(contains("createdBy", createdByEmail))
      .limit(limit)
      .build()

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
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(dataSourceFileSchema)
      .where(
        and([
          contains("fileName", fileName),
          contains("dataSourceId", dataSourceId),
          contains("uploadedBy", uploadedBy),
        ]),
      )
      .limit(1)
      .build()

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
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(dataSourceFileSchema)
      .where(
        and([
          contains("dataSourceName", dataSourceName),
          contains("uploadedBy", userEmail),
        ]),
      )
      .limit(0)
      .build()

    const countPayload = {
      yql,
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
      const yql = YqlBuilder.create({ requirePermissions: false })
        .from(dataSourceFileSchema)
        .where(
          and([
            contains("dataSourceName", dataSourceName),
            contains("uploadedBy", userEmail),
          ]),
        )
        .orderBy("createdAt", "desc")
        .build()

      const payload = {
        yql,
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
    params: SearchSlackParams & {
      hits: number
      entity: Entity | null
      profile?: SearchModes
    },
  ): YqlProfile => {
    const {
      hits,
      entity,
      profile = SearchModes.NativeRank,
      filterQuery: query,
      timestampRange,
      agentChannelIds: agentSelectedChannelIds,
      email,
      asc,
      mentions,
      channelIds,
      userIds,
      limit,
      offset,
    } = params

    const conditions: YqlCondition[] = query
      ? [
          or([
            userInput("@query", hits),
            nearestNeighbor("text_embeddings", "e", hits),
          ]),
        ]
      : []
    if (timestampRange && (timestampRange.from || timestampRange.to)) {
      conditions.push(
        timestamp("updatedAt", "updatedAt", {
          from: timestampRange.from,
          to: timestampRange.to,
        }),
      )
    }

    if (channelIds) {
      if (channelIds.length === 1 && channelIds[0]) {
        conditions.push(contains("channelId", channelIds[0]))
      } else {
        conditions.push(or(channelIds.map((id) => contains("channelId", id))))
      }
    }

    if (userIds) {
      if (userIds.length === 1 && userIds[0]) {
        conditions.push(contains("userId", userIds[0]))
      } else {
        conditions.push(or(userIds.map((id) => contains("userId", id))))
      }
    }

    // selected channels from agent
    if (agentSelectedChannelIds && agentSelectedChannelIds.length > 0) {
      if (agentSelectedChannelIds.length === 1 && agentSelectedChannelIds[0]) {
        conditions.push(contains("channelId", agentSelectedChannelIds[0]))
      } else {
        conditions.push(
          or(agentSelectedChannelIds.map((id) => contains("channelId", id))),
        )
      }
    }

    if (mentions && mentions.length > 0) {
      if (mentions.length === 1 && mentions[0]) {
        conditions.push(contains("mentions", mentions[0]))
      } else {
        conditions.push(
          or(mentions.map((mention) => contains("mentions", mention))),
        )
      }
    }

    if (!conditions.length) {
      return { yql: "" } as YqlProfile
    }

    const yqlBuilder = YqlBuilder.create({ email, requirePermissions: true })
      .from(chatMessageSchema)
      .where(and(conditions))

    // TODO: Improve ordering for query-based searches
    // currently getting error as
    // "Sorting is not supported with global phase" when using both query and nn search
    if (!query) {
      yqlBuilder.orderBy("updatedAt", asc ? "asc" : "desc")
    }

    if (offset) {
      yqlBuilder.offset(offset)
    }
    if (limit) {
      yqlBuilder.limit(limit)
    }

    return yqlBuilder.buildProfile(profile)
  }

  SearchVespaThreads = async (
    threadIdsInput: string[],
  ): Promise<VespaSearchResponse> => {
    const validThreadIds = threadIdsInput.filter(
      (id) => typeof id === "string" && id.length > 0,
    )

    if (validThreadIds.length === 0) {
      this.logger.warn("SearchVespaThreads called with no valid threadIds.")
      return vespaEmptyResponse()
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

  searchSlackMessages = async (
    params: SearchSlackParams,
  ): Promise<VespaSearchResponse> => {
    const {
      timestampRange = null,
      limit = this.config.page,
      offset = 0,
      email,
      user = null,
      asc,
      channelName = null,
      filterQuery = null,
      mentions,
    } = params

    let channelIds: string[] | undefined
    let userIds: string[] | undefined
    let agentSelectedChannelIds: string[] | undefined = params.agentChannelIds
    // Fetch channelId
    if (channelName) {
      try {
        const resp = (await this.vespa.searchSlackChannelByName(
          email,
          channelName,
        )) as VespaSearchResponse
        if (resp?.root?.children?.length > 0) {
          const results = resp.root.children
          channelIds = results
            .map((child) => (child.fields as VespaChatContainer).docId)
            .filter(Boolean)
        }
      } catch (e) {
        this.logger.error(
          `Could not fetch channelId for channel: ${channelName}`,
          e,
        )
      }
    }

    // Fetch userId
    if (user) {
      try {
        const resp = (await this.vespa.searchChatUser(
          email,
          user,
        )) as VespaSearchResponse
        if (resp?.root?.children?.length > 0) {
          const results = resp.root.children
          if (results.length > 0) {
            userIds = results
              .map((child) => (child.fields as VespaChatUser).docId)
              .filter(Boolean)
          }
        }
      } catch (e) {
        this.logger.error(`Could not fetch userId for user: ${user}`, e)
      }
    }

    let mentionedUserIds: string[] = []
    if (mentions && mentions.length > 0) {
      try {
        const resp = (await this.vespa.searchChatUser(
          email,
          mentions,
        )) as VespaSearchResponse
        const results = resp?.root?.children
        if (results?.length > 0) {
          const maxMentions = Math.min(results.length, mentions.length)
          // Map mentions to user IDs
          mentionedUserIds = results
            .map((child) => (child.fields as VespaChatUser).docId)
            .filter(Boolean)
            .slice(0, maxMentions)
        }
      } catch (e) {
        this.logger.error(
          `Could not fetch userId for mentions: ${mentions.join(", ")}`,
          e,
        )
      }
    }

    // Hybrid filterQuery-based search
    const { yql, profile } = this.SlackHybridProfile({
      hits: limit,
      entity: SlackEntity.Message,
      profile: SearchModes.NativeRank,
      filterQuery: filterQuery ?? "",
      timestampRange,
      email: email!,
      asc,
      mentions: mentionedUserIds.length > 0 ? mentionedUserIds : undefined,
      channelIds,
      userIds,
      agentChannelIds: agentSelectedChannelIds,
      offset,
    })
    // console.log(
    //   "Vespa YQL Query in searchSlackMessages: ",
    //   formatYqlToReadable(yql),
    // )
    if (!yql || yql.trim() === "") {
      return vespaEmptyResponse()
    }

    const hybridPayload = {
      yql,
      query: filterQuery,
      email: email,
      "ranking.profile": filterQuery ? profile : "unranked",
      "input.query(e)": "embed(@query)",
      "input.query(alpha)": 0.5,
      "input.query(recency_decay_rate)": 0.1,
      hits: limit,
      timeout: "20s",
      ...(offset && { offset }),
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

  /**
   * Fetches Slack channel or user information based on the passed entity type
   * @param entity - The Slack entity type (SlackEntity.Channel or SlackEntity.User)
   * @param identifier - Optional identifier to search for (email for users, channel name/id for channels). If empty, returns all with pagination
   * @param requestingUserEmail - Email of the user making the request (for permissions)
   * @param app - The app type (should be Apps.Slack)
   * @param limit - Number of results to return (default: 50)
   * @param offset - Offset for pagination (default: 0)
   * @returns Promise<VespaSearchResponse> containing the matching Slack entities
   */
  fetchSlackEntity = async (
    entity: SlackEntity,
    identifier: string | null | undefined,
    requestingUserEmail: string,
    app: Apps = Apps.Slack,
    limit: number = 50,
    offset: number = 0,
  ): Promise<VespaSearchResponse> => {
    if (app !== Apps.Slack) {
      throw new Error(`Expected app to be Slack, but received: ${app}`)
    }

    // Validate entity type for the generic function
    if (entity !== SlackEntity.User && entity !== SlackEntity.Channel) {
      throw new Error(`Unsupported Slack entity type: ${entity}`)
    }

    try {
      // Use the generic function for both users and channels
      return await this.fetchSlackEntities(
        entity as SlackEntity.User | SlackEntity.Channel,
        identifier,
        requestingUserEmail,
        limit,
        offset,
      )
    } catch (error) {
      const isListAll = !identifier || identifier.trim().length === 0
      const trimmedIdentifier = identifier?.trim() || ""

      this.logger.error(
        `Error fetching Slack ${entity}${isListAll ? " (list all)" : ` with identifier "${trimmedIdentifier}"`}`,
        error,
      )
      throw new ErrorPerformingSearch({
        cause: error as Error,
        sources:
          entity === SlackEntity.User ? chatUserSchema : chatContainerSchema,
        message: `Failed to fetch Slack ${entity}`,
      })
    }
  }

  // Fetch Slack entities to list the channel name or users of slack
  // No permissions check when listing or searching the slack User because we don't have permissions fields in that
  // But while fetching slack channels we are adding the permission check
  private fetchSlackEntities = async (
    entity: SlackEntity.User | SlackEntity.Channel,
    identifier: string | null | undefined,
    requestingUserEmail: string,
    limit: number,
    offset: number = 0,
  ): Promise<VespaSearchResponse> => {
    const isListAll = !identifier || identifier.trim().length === 0
    const trimmedIdentifier = identifier?.trim() || ""

    // Entity-specific configuration
    const entityConfig = {
      [SlackEntity.User]: {
        schema: chatUserSchema as VespaSchema,
        searchFields: ["email", "name", "docId"],
        vectorField: "user_embeddings",
        entityName: "user",
      },
      [SlackEntity.Channel]: {
        schema: chatContainerSchema as VespaSchema,
        searchFields: ["name", "channelName", "docId"],
        vectorField: "channel_embeddings",
        entityName: "channel",
      },
    }

    const config = entityConfig[entity]
    if (!config) {
      throw new Error(`Unsupported Slack entity type: ${entity}`)
    }

    let yql: string
    let searchPayload: any

    if (isListAll) {
      // List all entities with pagination
      yql = YqlBuilder.create({
        sources: [config.schema],
        requirePermissions: entity == SlackEntity.Channel ? true : false,
        email: entity == SlackEntity.Channel ? requestingUserEmail : undefined,
      })
        .from(config.schema)
        .where(and([contains("app", Apps.Slack), contains("entity", entity)]))
        .limit(limit)
        .offset(offset)
        .build()

      searchPayload = {
        yql,
        hits: limit,
        offset,
        timeout: "15s",
      }
      // console.log(yql)

      this.logger.info(
        `Fetching all Slack ${config.entityName}s (page ${Math.floor(offset / limit) + 1})`,
      )
    } else {
      // BM25 text search only
      yql = YqlBuilder.create({
        sources: [config.schema],
        requirePermissions: entity == SlackEntity.Channel ? true : false,
        email: entity == SlackEntity.Channel ? requestingUserEmail : undefined,
      })
        .from(config.schema)
        .where(
          or([
            userInput("@query", limit), // BM25 text search
            ...config.searchFields.map((field) =>
              matches(field, trimmedIdentifier),
            ), // Pattern matches
          ]),
        )
        .limit(limit)
        .build()
      searchPayload = {
        yql,
        query: trimmedIdentifier,
        hits: limit,
        "ranking.profile": SearchModes.BM25,
        timeout: "10s",
      }

      this.logger.info(
        `Using BM25 text search for Slack ${config.entityName}: "${trimmedIdentifier}"`,
      )
    }

    const response = await this.vespa.search<VespaSearchResponse>(searchPayload)

    this.logger.info(
      `Fetched ${response.root?.children?.length || 0} Slack ${config.entityName}s${
        isListAll
          ? ` (page ${Math.floor(offset / limit) + 1})`
          : ` for identifier: "${trimmedIdentifier}"`
      }`,
    )

    return response
  }

  getFolderItems = async (
    docIds: string[],
    schema: VespaSchema,
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

    // Construct RAG YQL query - hybrid search with both text and vector search
    // This combines BM25 text search with vector similarity search
    const conditions: YqlCondition[] = [
      or([
        userInput("@query", limit),
        nearestNeighbor("chunk_embeddings", "e", limit),
      ]),
    ]

    if (docIds && docIds.length > 0) {
      const docIdConditions = docIds.map((id) => contains("docId", id.trim()))
      conditions.push(or(docIdConditions))
    }

    if (parentDocIds && parentDocIds.length > 0) {
      const parentDocIdConditions = parentDocIds.map((id) =>
        contains("clFd", id.trim()),
      )
      conditions.push(or(parentDocIdConditions))
    }

    // Don't require permission checks for KB items
    const yql = YqlBuilder.create({ requirePermissions: false })
      .from(KbItemsSchema)
      .where(and(conditions))
      .build()

    const searchPayload = {
      yql: yql,
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

  searchGoogleApps = async ({
    app,
    email,
    query,
    limit = this.config.page,
    offset = 0,
    sortBy = "desc",
    labels,
    timeRange,
    participants,
    owner,
    attendees,
    eventStatus,
    excludeDocIds,
    docIds,
    driveEntity,
    alpha = 0.5,
    rankProfile = SearchModes.NativeRank,
  }: SearchGoogleAppsParams): Promise<VespaSearchResponse> => {
    const appToSourceMap: Record<GoogleApps, VespaSchema | VespaSchema[]> = {
      [GoogleApps.Gmail]: [mailSchema, mailAttachmentSchema],
      [GoogleApps.Drive]: fileSchema,
      [GoogleApps.Calendar]: eventSchema,
      [GoogleApps.Contacts]: userSchema,
    }

    const conditions: YqlCondition[] = []
    const sources = appToSourceMap[app]

    // don't need hybrid search for contacts
    if (query && query.trim().length > 0 && app !== GoogleApps.Contacts) {
      conditions.push(
        or([
          userInput("@query", limit),
          nearestNeighbor("chunk_embeddings", "e", limit),
        ]),
      )
    }
    const timestampField =
      app === GoogleApps.Gmail
        ? "timestamp"
        : app === GoogleApps.Drive
          ? "updatedAt"
          : app === GoogleApps.Calendar
            ? "startTime"
            : "updatedAt"
    if (timeRange && (timeRange.startTime || timeRange.endTime)) {
      conditions.push(
        timestamp(timestampField, timestampField, {
          from: timeRange.startTime ?? null,
          to: timeRange.endTime ?? null,
        }),
      )
    }

    // Gmail-specific
    if (app === GoogleApps.Gmail) {
      if (labels && labels.length > 0) {
        const labelConditions = labels.map((label) =>
          contains("labels", label.trim()),
        )
        conditions.push(or(labelConditions))
      }
      if (participants) {
        const participantConditions = getGmailParticipantsConditions(
          participants,
          this.logger,
        )
        if (participantConditions.length > 0) {
          conditions.push(and(participantConditions))
        }
      }
    }

    // Drive-specific
    if (app === GoogleApps.Drive) {
      if (owner) {
        conditions.push(
          isValidEmail(owner)
            ? contains("owner", owner)
            : matches("owner", owner),
        )
      }
      if (driveEntity) {
        if (Array.isArray(driveEntity)) {
          const entityConditions = driveEntity.map((entity) =>
            contains("entity", entity),
          )
          conditions.push(or(entityConditions))
        } else {
          conditions.push(contains("entity", driveEntity))
        }
      }
    }

    // Calendar-specific
    if (app === GoogleApps.Calendar) {
      if (attendees && attendees.length > 0) {
        const attendeeConditions = attendees.map((attendee) =>
          isValidEmail(attendee)
            ? contains("attendeesNames", attendee)
            : matches("attendeesNames", attendee),
        )
        conditions.push(or(attendeeConditions))
      }

      if (eventStatus) {
        conditions.push(contains("status", eventStatus))
      }
    }

    // Contacts-specific
    if (app === GoogleApps.Contacts) {
      if (query && query.trim().length > 0) {
        conditions.push(
          isValidEmail(query)
            ? contains("email", query)
            : or([matches("email", query), matches("name", query)]),
        )
      }
    }

    const yqlBuilder = YqlBuilder.create({ email, requirePermissions: true })
      .from(sources)
      .limit(limit)
    // .orderBy(timestampField, sortBy)

    if (conditions.length > 0) {
      yqlBuilder.where(and(conditions))
    }

    if (excludeDocIds && excludeDocIds.length > 0) {
      yqlBuilder.excludeDocIds(excludeDocIds)
    }
    if (docIds && docIds.length > 0) {
      yqlBuilder.includeDocIds(docIds)
    }

    const yql = yqlBuilder.build()
    // console.log(
    //   "Vespa YQL Query in searchGoogleApps: ",
    //   formatYqlToReadable(yql),
    // )
    const searchPayload = {
      yql: yql,
      ...(query
        ? {
            query: query.trim(),
            "input.query(e)": "embed(@query)",
          }
        : {}),
      "ranking.profile": rankProfile,
      "input.query(alpha)": alpha,
      hits: limit,
      offset,
      timeout: "30s",
    }

    try {
      const response =
        await this.vespa.search<VespaSearchResponse>(searchPayload)
      this.logger.info(
        `[searchGoogleApps] Found ${response.root?.children?.length || 0} documents`,
      )

      return response
    } catch (error) {
      const searchError = new ErrorPerformingSearch({
        cause: error as Error,
        sources: Array.isArray(sources) ? sources.join(", ") : sources,
        message: `searchGoogleApps failed for app: ${app}`,
      })
      this.logger.error(searchError, "Error in searchGoogleApps function")
      throw searchError
    }
  }
}
