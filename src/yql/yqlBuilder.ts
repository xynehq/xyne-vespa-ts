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
import {
  or,
  exclude,
  include,
  and,
  contains,
  andWithoutPermissions,
  orWithoutPermissions,
  andWithPermissions,
  orWithPermissions,
} from "."
import { PermissionCondition, PermissionWrapper } from "./permissions"
import {
  Apps,
  Entity,
  KbItemsSchema,
  SearchModes,
  userSchema,
  VespaSchema,
} from "../types"
import { YqlProfile } from "./types"

export class YqlBuilder {
  private selectClause: string = "select *"
  private sourcesClause: string = ""
  private whereConditions: YqlCondition[] = []
  private currentSources: VespaSchema[] = []
  private limitClause?: number
  private offsetClause?: number
  private timeoutClause?: string
  private groupByClause?: string
  private appCondition?: YqlCondition
  private entityCondition?: YqlCondition
  private excludeDocIdCondtion?: YqlCondition
  private includeDocIdCondtion?: YqlCondition
  private orderByClause?: string

  private permissionWrapper?: PermissionWrapper
  private options: Required<YqlBuilderOptions>
  private withPermissions: boolean
  private userEmail?: string
  constructor(options: Partial<YqlBuilderOptions> = {}) {
    this.withPermissions = !!(options.email && options.email.trim())

    this.options = {
      email: options.email || "",
      sources: options.sources || [],
      targetHits: options.targetHits || 10,
      limit: options.limit || 100,
      offset: options.offset || 0,
      timeout: options.timeout || "2s",
      requirePermissions:
        this.withPermissions && options.requirePermissions !== false,
      validateSyntax: options.validateSyntax !== true,
    }
    this.userEmail = options.email
  }

  /**
   * Set the fields to select in the query
   */
  select(fields: string | string[] = "*"): this {
    if (Array.isArray(fields)) {
      if (fields.length === 0) {
        throw new Error("At least one field must be specified")
      }

      this.selectClause = `select ${fields.join(", ")}`
    } else {
      if (!fields || typeof fields !== "string" || !fields.trim()) {
        throw new Error("Invalid field specification")
      }
      this.selectClause = `select ${fields}`
    }

    return this
  }

  /**
   * Helper to create And conditions with proper permission settings
   */
  private createAnd(
    conditions: YqlCondition[],
    isPermissionBypassed: boolean = false,
  ): And {
    if (this.withPermissions && this.userEmail && !isPermissionBypassed) {
      return this.createPermissionAwareAnd(conditions)
    }
    return andWithoutPermissions(conditions)
  }

  private createPermissionAwareAnd(conditions: YqlCondition[]): And {
    const includesUserSchema = this.currentSources.includes(userSchema)
    const isSingleUserSchema =
      this.currentSources.length === 1 && this.currentSources[0] === userSchema

    if (isSingleUserSchema) {
      // Only owner check for user schema
      return And.withOwnerPermissions(conditions)
    } else if (includesUserSchema) {
      // Include both owner and permissions check for user and other schemas
      return andWithPermissions(conditions)
    } else {
      // Only permissions check for non-user schemas
      return And.withEmailPermissions(conditions)
    }
  }

  /**
   * Helper to create Or conditions with proper permission settings
   */
  private createOr(
    conditions: YqlCondition[],
    isPermissionBypassed: boolean = false,
  ): Or {
    if (this.withPermissions && this.userEmail && !isPermissionBypassed) {
      return this.createPermissionAwareOr(conditions)
    }
    return orWithoutPermissions(conditions)
  }

  /**
   * Creates Or condition with conditional permission logic based on sources
   */
  private createPermissionAwareOr(conditions: YqlCondition[]): Or {
    const includesUserSchema = this.currentSources.includes(userSchema)
    const isSingleUserSchema =
      this.currentSources.length === 1 && this.currentSources[0] === userSchema

    if (isSingleUserSchema) {
      // Only owner check for user schema
      return Or.withOwnerPermissions(conditions)
    } else if (includesUserSchema) {
      // Include both owner and permissions check for user and other schemas
      return orWithPermissions(conditions)
    } else {
      // Only permissions check for non-user schemas
      return Or.withEmailPermissions(conditions)
    }
  } /**
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

    this.currentSources = sourceArray as VespaSchema[]
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
      this.whereConditions.push(this.createAnd(conditions).parenthesize())
    }
    return this
  }

  /**
   * Add multiple WHERE conditions with OR logic
   */
  whereOr(...conditions: YqlCondition[]): this {
    if (conditions.length > 0) {
      this.whereConditions.push(this.createOr(conditions).parenthesize())
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

    return this.where(this.createOr([userInput, vectorSearch]).parenthesize())
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
      this.appCondition = this.createAnd([contains("app", apps[0]!)])
    } else if (apps.length > 1) {
      const conditions = apps.map((a) => contains("app", a))
      this.appCondition = this.createOr(conditions)
    }

    return this
  }

  /**
   * Add entity filter condition
   */
  filterByEntity(entity: Entity | Entity[]): this {
    const entities = Array.isArray(entity) ? entity : [entity]

    if (entities.length === 1) {
      this.entityCondition = this.createAnd([contains("entity", entities[0]!)])
    } else if (entities.length > 1) {
      const conditions = entities.map((e) => contains("entity", e))
      this.entityCondition = this.createOr(conditions)
    }

    return this
  }

  /**
   * Add exclusion condition for document IDs
   */
  excludeDocIds(docIds: string[]): this {
    const exclusion = exclude(docIds)
    if (!exclusion.isEmpty()) {
      this.excludeDocIdCondtion = exclusion
    }
    return this
  }

  /**
   * Add inclusion condition for document IDs
   */
  includeDocIds(docIds: string[]): this {
    const inclusion = include("docId", docIds)
    if (!inclusion.isEmpty()) {
      this.includeDocIdCondtion = inclusion
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
  orderBy(field: string, direction: "asc" | "desc" = "asc"): this {
    if (!field || typeof field !== "string" || !field.trim()) {
      throw new Error("Field name is required for ORDER BY")
    }

    if (direction !== "asc" && direction !== "desc") {
      throw new Error("Direction must be 'asc' or 'desc'")
    }

    this.orderByClause = `${field.trim()} ${direction}`
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

    // Build WHERE clause with permission management
    if (
      this.whereConditions.length > 0 ||
      this.appCondition ||
      this.entityCondition ||
      (this.withPermissions && this.userEmail)
    ) {
      const whereClause = this.buildWhereClause()
      if (whereClause) {
        yql += ` where (${whereClause})`
      }
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

    // Replace @email placeholder with actual email if available
    if (this.userEmail && this.userEmail.trim()) {
      yql = yql.replace(/@email/g, `${this.userEmail}`)
    }

    return yql
  }

  /**
   * Build WHERE clause with intelligent permission management
   */
  private buildWhereClause(): string | null {
    const allConditions: YqlCondition[] = []

    if (this.whereConditions.length > 0) {
      let combinedConditions: YqlCondition
      // Don't add permissions here - let recursivelyApplyPermissions handle it
      if (this.whereConditions.length === 1) {
        combinedConditions = this.whereConditions[0]!
      } else {
        combinedConditions = Or.withoutPermissions(this.whereConditions)
      }
      allConditions.push(combinedConditions)
    }

    // App filter
    if (this.appCondition) {
      allConditions.push(this.appCondition)
    }

    // Entity filter
    if (this.entityCondition) {
      allConditions.push(this.entityCondition)
    }

    // Exclude doc IDs filter
    if (this.excludeDocIdCondtion) {
      allConditions.push(this.excludeDocIdCondtion)
    }
    // Include doc IDs filter
    if (this.includeDocIdCondtion) {
      allConditions.push(this.includeDocIdCondtion)
    }

    // If we have permissions enabled but no conditions, add just permissions
    if (allConditions.length === 0 && this.withPermissions && this.userEmail) {
      const permissionCondition = this.buildPermissionCondition()
      return permissionCondition ? permissionCondition.toString() : null
    }

    if (allConditions.length === 0) {
      return null
    }

    // Combine all conditions with AND
    let finalCondition: YqlCondition
    if (allConditions.length === 1) {
      finalCondition = allConditions[0]!
    } else {
      finalCondition = this.createAnd(allConditions)
    }

    const isOnlyKbSource =
      this.currentSources.length === 1 &&
      this.currentSources[0] === KbItemsSchema
    // Apply permissions only at the top level if permissions are enabled
    // we also we need to skip permission checks for kb_items
    if (this.withPermissions && this.userEmail && !isOnlyKbSource) {
      const processedCondition = this.applyTopLevelPermissions(finalCondition)
      return processedCondition.toString()
    }

    return this.parenthesisConditions(finalCondition).toString()
  }

  private parenthesisConditions(condition: YqlCondition): YqlCondition {
    return this.recursivelyApplyParentheses(condition)
  }

  /**
   * Recursively apply parentheses to all OR and AND conditions in the tree
   */
  private recursivelyApplyParentheses(condition: YqlCondition): YqlCondition {
    if (condition instanceof And) {
      const processedConditions = condition
        .getConditions()
        .map((child) => this.recursivelyApplyParentheses(child))

      return and(processedConditions).parenthesize()
    }

    if (condition instanceof Or) {
      const processedConditions = condition
        .getConditions()
        .map((child) => this.recursivelyApplyParentheses(child))
      // Create new Or condition from processed conditions and parenthesize it
      return or(processedConditions).parenthesize()
    }

    if (condition instanceof Timestamp) {
      return condition.parenthesize()
    }
    // Leaf nodes, return as-is (parentheses will be added by parent conditions as needed)
    return condition
  }

  /**
   * Apply permissions to each OR condition individually
   */
  private applyTopLevelPermissions(condition: YqlCondition): YqlCondition {
    return this.recursivelyApplyPermissions(condition)
  }

  /**
   * Recursively apply permissions to all OR and AND conditions in the tree
   */
  private recursivelyApplyPermissions(condition: YqlCondition): YqlCondition {
    // processing each condition based on its type
    if (condition instanceof And) {
      // Check if this AND condition was explicitly created without permissions
      if (condition.isPermissionBypassed()) {
        const processedConditions = condition
          .getConditions()
          .map((child) => this.recursivelyApplyPermissions(child))
        return And.withoutPermissions(processedConditions).parenthesize()
      }

      const processedConditions = condition
        .getConditions()
        .map((child) => this.recursivelyApplyPermissions(child))
      // won't require permissions for AND children as parent OR will handle it
      // Top level ANDs will contains permissions
      return this.createAnd(processedConditions).parenthesize()
    }

    // If it's an Or condition, wrap it with permissions and process children
    if (condition instanceof Or) {
      // Check if this OR condition was explicitly created without permissions
      if (condition.isPermissionBypassed()) {
        const processedConditions = condition
          .getConditions()
          .map((child) => this.recursivelyApplyPermissions(child))
        return Or.withoutPermissions(processedConditions).parenthesize()
      }

      // First, recursively process any nested OR conditions within this OR
      const processedConditions = condition
        .getConditions()
        .map((child) => this.recursivelyApplyPermissions(child))

      return this.createOr(processedConditions).parenthesize()
    }

    if (condition instanceof Timestamp) {
      return condition.parenthesize()
    }
    // Leaf nodes, return as-is (permissions will be added by parent)
    return condition
  }

  /**
   * Build permission condition based on current sources
   */
  private buildPermissionCondition(): YqlCondition | null {
    if (!this.userEmail) {
      return null
    }

    const includesUserSchema = this.currentSources.includes(userSchema)
    const isSingleUserSchema =
      this.currentSources.length === 1 && this.currentSources[0] === userSchema

    if (isSingleUserSchema) {
      // Only owner check for single user schema
      return contains("owner", this.userEmail)
    } else if (includesUserSchema) {
      // Both owner and permissions check for mixed sources with user schema
      return or([
        contains("owner", this.userEmail),
        contains("permissions", this.userEmail),
      ]).parenthesize()
    } else {
      // Only permissions check for non-user schemas
      return contains("permissions", this.userEmail)
    }
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
  static create(options?: Partial<YqlBuilderOptions>): YqlBuilder {
    return new YqlBuilder(options)
  }
}
