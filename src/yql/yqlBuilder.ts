import {
  YqlBuilderOptions,
  TimestampRange,
  FieldName,
  FieldValue,
  YqlCondition,
} from "./types"
import {
  BaseCondition,
  VespaField,
  UserInput,
  NearestNeighbor,
  And,
  Or,
  Timestamp,
  Exclude,
  Include,
  escapeYqlValue,
} from "./conditions"
import { or, exclude, include, and, contains } from "."
import { PermissionCondition, PermissionWrapper } from "./permissions"
import { Apps, Entity, SearchModes, VespaSchema } from "../types"
import { YqlProfile } from "./types"

export class YqlBuilder {
  private selectClause: string = "select *"
  private sourcesClause: string = ""
  private whereConditions: YqlCondition[] = []
  private limitClause?: number
  private offsetClause?: number
  private timeoutClause?: string
  private groupByClause?: string
  private orderByClause?: string

  private permissionWrapper: PermissionWrapper
  private options: Required<YqlBuilderOptions>

  constructor(
    private userEmail: string,
    options: Partial<YqlBuilderOptions> = {},
  ) {
    if (!userEmail || !userEmail.trim()) {
      throw new Error("User email is required for YQL builder")
    }

    this.options = {
      sources: options.sources || [],
      targetHits: options.targetHits || 10,
      limit: options.limit || 100,
      offset: options.offset || 0,
      timeout: options.timeout || "2s",
      requirePermissions: options.requirePermissions !== false, // Default to true
      validateSyntax: options.validateSyntax !== true, // Default to false
    }

    this.permissionWrapper = new PermissionWrapper(userEmail)
  }

  /**
   * Set the sources for the query
   */
  from(sources: VespaSchema | VespaSchema[] | "*"): this {
    const sourceArray = Array.isArray(sources) ? sources : [sources]

    if (sourceArray.length === 0) {
      throw new Error("At least one source is required")
    }

    // Validate source names
    sourceArray.forEach((source) => {
      if (!source || typeof source !== "string" || !source.trim()) {
        throw new Error("Invalid source name")
      }
    })

    this.sourcesClause = `from sources ${sourceArray.join(", ")}`
    return this
  }

  /**
   * Add a WHERE condition
   */
  where(condition: YqlCondition): this {
    this.whereConditions.push(condition)
    return this
  }

  /**
   * Add multiple WHERE conditions with AND logic
   */
  whereAnd(...conditions: YqlCondition[]): this {
    if (conditions.length > 0) {
      this.whereConditions.push(and(conditions, true).parenthesize())
    }
    return this
  }

  /**
   * Add multiple WHERE conditions with OR logic
   */
  whereOr(...conditions: YqlCondition[]): this {
    if (conditions.length > 0) {
      this.whereConditions.push(or(conditions).parenthesize())
    }
    return this
  }

  /**
   * Add user input search condition
   */
  userInput(queryParam: string = "@query", targetHits?: number): this {
    const hits = targetHits || this.options.targetHits
    return this.where(new UserInput(queryParam, hits))
  }

  /**
   * Add nearest neighbor vector search condition
   */
  nearestNeighbor(
    field: FieldName,
    queryParam: string = "e",
    targetHits?: number,
  ): this {
    const hits = targetHits || this.options.targetHits
    return this.where(new NearestNeighbor(field, queryParam, hits))
  }

  /**
   * Add hybrid search condition (user input OR nearest neighbor)
   */
  hybridSearch(
    vectorField: FieldName = "chunk_embeddings",
    queryParam: string = "@query",
    vectorParam: string = "e",
    targetHits?: number,
  ): this {
    const hits = targetHits || this.options.targetHits
    const userInput = new UserInput(queryParam, hits)
    const vectorSearch = new NearestNeighbor(vectorField, vectorParam, hits)

    return this.where(or([userInput, vectorSearch]).parenthesize())
  }

  /**
   * Add timestamp range condition
   */
  timestampRange(
    fromField: FieldName,
    toField: FieldName,
    range: TimestampRange,
  ): this {
    if (
      (range.from !== null && range.from !== undefined) ||
      (range.to !== null && range.to !== undefined)
    ) {
      return this.where(new Timestamp(fromField, toField, range).parenthesize())
    }
    return this
  }

  /**
   * Add app filter condition
   */
  filterByApp(app: Apps | Apps[]): this {
    const apps = Array.isArray(app) ? app : [app]

    if (apps.length === 1) {
      return this.where(contains("app", apps[0]!))
    } else if (apps.length > 1) {
      const conditions = apps.map((a) => contains("app", a))
      return this.where(or(conditions).parenthesize())
    }

    return this
  }

  /**
   * Add entity filter condition
   */
  filterByEntity(entity: Entity | Entity[]): this {
    const entities = Array.isArray(entity) ? entity : [entity]

    if (entities.length === 1) {
      return this.where(contains("entity", entities[0]!))
    } else if (entities.length > 1) {
      const conditions = entities.map((e) => contains("entity", e))
      return this.where(or(conditions).parenthesize())
    }

    return this
  }

  /**
   * Add exclusion condition for document IDs
   */
  excludeDocIds(docIds: string[]): this {
    const exclusion = exclude(docIds)
    if (!exclusion.isEmpty()) {
      return this.where(exclusion)
    }
    return this
  }

  /**
   * Add inclusion condition for document IDs
   */
  includeDocIds(docIds: string[]): this {
    const inclusion = include("docId", docIds)
    if (!inclusion.isEmpty()) {
      return this.where(inclusion)
    }
    return this
  }

  /**
   * Add inclusion condition for any field
   */
  includeValues(field: FieldName, values: string[]): this {
    const inclusion = include(field, values)
    if (!inclusion.isEmpty()) {
      return this.where(inclusion)
    }
    return this
  }

  /**
   * Add mail label exclusion (Gmail specific)
   */
  excludeMailLabels(labels: string[]): this {
    if (!labels || labels.length === 0) {
      return this
    }

    const labelConditions = labels
      .filter((label) => label && label.trim())
      .map((label) => contains("labels", label.trim()))

    if (labelConditions.length > 0) {
      const combinedLabels =
        labelConditions.length === 1
          ? labelConditions[0]!
          : or(labelConditions).parenthesize()

      return this.where(combinedLabels.not())
    }

    return this
  }

  /**
   * Set LIMIT clause
   */
  limit(limit: number): this {
    if (limit < 0) {
      throw new Error("Limit must be non-negative")
    }
    this.limitClause = limit
    return this
  }

  /**
   * Set OFFSET clause
   */
  offset(offset: number): this {
    if (offset < 0) {
      throw new Error("Offset must be non-negative")
    }
    this.offsetClause = offset
    return this
  }

  /**
   * Set timeout
   */
  timeout(timeout: string): this {
    this.timeoutClause = timeout
    return this
  }

  /**
   * Add GROUP BY clause
   */
  groupBy(clause: string): this {
    this.groupByClause = clause
    return this
  }

  /**
   * Add ORDER BY clause
   */
  orderBy(clause: string): this {
    this.orderByClause = clause
    return this
  }

  /**
   * Build the final YQL query
   */
  build(): string {
    if (!this.sourcesClause) {
      throw new Error("Sources must be specified using from()")
    }

    let yql = `${this.selectClause} ${this.sourcesClause}`

    // Build WHERE clause with permission wrapping
    if (this.whereConditions.length > 0) {
      const combinedConditions =
        this.whereConditions.length === 1
          ? this.whereConditions[0]!
          : and(this.whereConditions)

      yql += ` where (${combinedConditions.toString()})`
    }

    // Add other clauses
    if (this.limitClause !== undefined) {
      yql += ` limit ${this.limitClause}`
    }

    if (this.offsetClause !== undefined && this.offsetClause > 0) {
      yql += ` offset ${this.offsetClause}`
    }

    if (this.groupByClause) {
      yql += ` | ${this.groupByClause}`
    }

    if (this.orderByClause) {
      yql += ` order by ${this.orderByClause}`
    }

    // Validate syntax if enabled
    if (this.options.validateSyntax) {
      this.validateYqlSyntax(yql)
    }

    return yql
  }

  /**
   * Build YQL profile object
   */
  buildProfile(profile: SearchModes = SearchModes.NativeRank): YqlProfile {
    return {
      profile: profile,
      yql: this.build(),
    }
  }

  /**
   * Basic YQL syntax validation
   */
  private validateYqlSyntax(yql: string): void {
    const requiredKeywords = ["select", "from"]
    const missingKeywords = requiredKeywords.filter(
      (keyword) => !new RegExp(`\\b${keyword}\\b`, "i").test(yql),
    )

    if (missingKeywords.length > 0) {
      throw new Error(
        `Missing required YQL keywords: ${missingKeywords.join(", ")}`,
      )
    }

    // Check for balanced parentheses
    let parenCount = 0
    for (const char of yql) {
      if (char === "(") parenCount++
      if (char === ")") parenCount--
      if (parenCount < 0) {
        throw new Error("Unmatched closing parenthesis in YQL")
      }
    }

    if (parenCount !== 0) {
      throw new Error("Unmatched opening parenthesis in YQL")
    }

    // Check for balanced quotes
    let inSingleQuote = false
    let inDoubleQuote = false
    let prevChar = ""

    for (const char of yql) {
      if (char === "'" && prevChar !== "\\" && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote
      }
      if (char === '"' && prevChar !== "\\" && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote
      }
      prevChar = char
    }

    if (inSingleQuote) {
      throw new Error("Unmatched single quote in YQL")
    }

    if (inDoubleQuote) {
      throw new Error("Unmatched double quote in YQL")
    }
  }

  /**
   * Create a new builder instance
   */
  static create(
    userEmail: string,
    options?: Partial<YqlBuilderOptions>,
  ): YqlBuilder {
    return new YqlBuilder(userEmail, options)
  }
}
