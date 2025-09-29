import { PermissionCondition } from "./permissions"
import {
  YqlCondition,
  LogicalOperator,
  SearchCondition,
  TimestampRange,
  FieldName,
  FieldValue,
  PermissionFieldType,
  PermissionOptions,
  Operator,
} from "./types"

/**
 * Escapes YQL values to prevent injection attacks and syntax errors
 */
export function escapeYqlValue(value: string | number | boolean): string {
  if (typeof value === "string") {
    // Escape single quotes and backslashes
    return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")
  }
  return String(value)
}

/**
 * Base class for all YQL conditions
 */
export abstract class BaseCondition implements YqlCondition {
  readonly __brand: "YqlCondition" = "YqlCondition"
  abstract toString(): string

  and(other: YqlCondition): And {
    return new And([this, other])
  }

  or(other: YqlCondition): Or {
    return new Or([this, other])
  }

  not(): Not {
    return new Not(this)
  }

  parenthesize(): Parenthesized {
    return new Parenthesized(this)
  }
}

/**
 * Simple field condition (field operator value)
 */
export class VespaField extends BaseCondition {
  constructor(
    private field: FieldName,
    private operator: string,
    private value: FieldValue,
  ) {
    super()
    this.validateFieldName(field)
  }

  private validateFieldName(field: string): void {
    if (!field || typeof field !== "string") {
      throw new Error("Field name must be a non-empty string")
    }
    // Basic validation for field names
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field)) {
      throw new Error(`Invalid field name: ${field}`)
    }
  }

  toString(): string {
    const escapedValue =
      typeof this.value === "string"
        ? `'${escapeYqlValue(this.value)}'`
        : String(this.value)
    return `${this.field} ${this.operator} ${escapedValue}`
  }

  static contains(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.CONTAINS, value)
  }

  static matches(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.MATCHES, value)
  }

  static equals(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.EQUAL, value)
  }

  static greaterThan(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.GREATER_THAN, value)
  }

  static greaterThanOrEqual(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.GREATER_THAN_OR_EQUAL, value)
  }

  static lessThan(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.LESS_THAN, value)
  }

  static lessThanOrEqual(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, Operator.LESS_THAN_OR_EQUAL, value)
  }
}

/**
 * FuzzyContains condition for YQL fuzzy search queries.
 * Example output: 'field contains ({maxEditDistance: 2, prefix: true} fuzzy(@query))'
 */
export class FuzzyContains extends BaseCondition {
  constructor(
    private field: string,
    private queryVar: string = "@query",
    private maxEditDistance: number = 2,
    private prefix: boolean = true,
  ) {
    super()
  }

  toString(): string {
    return `${this.field} contains ({maxEditDistance: ${this.maxEditDistance}, prefix: ${this.prefix}} fuzzy(${this.queryVar}))`
  }
}

/**
 * User input condition for text search
 */
export class UserInput extends BaseCondition {
  constructor(
    private queryParam: string = "@query",
    private targetHits: number,
  ) {
    super()
  }

  toString(): string {
    const hitsPrefix = `{targetHits:${this.targetHits}} `
    return `(${hitsPrefix}userInput(${this.queryParam}))`
  }
}

/**
 * Nearest neighbor condition for vector search
 */
export class NearestNeighbor extends BaseCondition {
  constructor(
    private field: FieldName,
    private queryParam: string = "e",
    private targetHits: number,
  ) {
    super()
  }

  toString(): string {
    const hitsPrefix = `{targetHits:${this.targetHits}} `
    return `(${hitsPrefix}nearestNeighbor(${this.field}, ${this.queryParam}))`
  }
}

/**
 * Logical AND condition
 */
export class And extends BaseCondition {
  private requirePermissions: boolean
  private userEmail: string
  private permissionType: PermissionFieldType
  private bypassPermissions: boolean
  constructor(
    private conditions: YqlCondition[],
    permissionOptions: PermissionOptions = {},
  ) {
    super()
    if (conditions.length === 0) {
      throw new Error("AND condition requires at least one condition")
    }

    this.requirePermissions = permissionOptions.requirePermissions ?? false
    this.userEmail = permissionOptions.userEmail ?? "@email"
    this.permissionType =
      permissionOptions.permissionType ?? PermissionFieldType.BOTH
    this.bypassPermissions = permissionOptions.bypassPermissions ?? false
  }

  toString(): string {
    const andCondition = this.conditions.map((c) => c.toString()).join(" and ")

    if (this.requirePermissions) {
      if (this.permissionType === PermissionFieldType.BOTH) {
        return `(${andCondition}) and (
                ${PermissionFieldType.OWNER} contains '${this.userEmail}' or 
                ${PermissionFieldType.PERMISSIONS} contains '${this.userEmail}'
                )`
      }
      const permissionField =
        this.permissionType === PermissionFieldType.OWNER
          ? PermissionFieldType.OWNER
          : PermissionFieldType.PERMISSIONS
      return `(${andCondition}) and ${permissionField} contains '${this.userEmail}'`
    }

    return andCondition
  }

  add(condition: YqlCondition): And {
    return new And([...this.conditions, condition], {
      requirePermissions: this.requirePermissions,
      userEmail: this.userEmail,
      permissionType: this.permissionType,
    })
  }

  /**
   * Get the conditions (for recursive processing)
   */
  getConditions(): YqlCondition[] {
    return [...this.conditions]
  }

  /**
   * Check if permission have been bypassed explicitly
   */
  isPermissionBypassed(): boolean {
    return this.bypassPermissions
  }

  /**
   * Creates an AndCondition with automatic email permission checking (permissions field) - explicit method
   */
  static withEmailPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): And {
    return new And(conditions, {
      requirePermissions: true,
      userEmail,
      permissionType: PermissionFieldType.PERMISSIONS,
    })
  }

  /**
   * Creates an AndCondition with automatic owner permission checking (owner field)
   */
  static withOwnerPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): And {
    return new And(conditions, {
      requirePermissions: true,
      userEmail,
      permissionType: PermissionFieldType.OWNER,
    })
  }

  /**
   * Creates an AndCondition without any permission checking
   */
  static withoutPermissions(conditions: YqlCondition[]): And {
    return new And(conditions, {
      requirePermissions: false,
      bypassPermissions: true,
    })
  }
}

/**
 * Logical OR condition with automatic email permission checking by default
 */
export class Or extends BaseCondition {
  private requirePermissions: boolean
  private userEmail: string
  private permissionType: PermissionFieldType
  private bypassPermissions: boolean

  constructor(
    private conditions: YqlCondition[],
    permissionOptions: PermissionOptions = {},
  ) {
    super()
    if (conditions.length === 0) {
      throw new Error("OR condition requires at least one condition")
    }

    this.requirePermissions = permissionOptions.requirePermissions ?? false
    this.userEmail = permissionOptions.userEmail ?? "@email"
    this.permissionType =
      permissionOptions.permissionType ?? PermissionFieldType.BOTH
    this.bypassPermissions = permissionOptions.bypassPermissions ?? false
  }

  toString(): string {
    const orCondition = this.conditions.map((c) => c.toString()).join(" or ")

    if (this.requirePermissions) {
      if (this.permissionType === PermissionFieldType.BOTH) {
        return `(${orCondition}) and (
                ${PermissionFieldType.OWNER} contains '${this.userEmail}' or
                ${PermissionFieldType.PERMISSIONS} contains '${this.userEmail}'
                )`
      }
      const permissionField =
        this.permissionType === PermissionFieldType.OWNER
          ? PermissionFieldType.OWNER
          : PermissionFieldType.PERMISSIONS
      return `(${orCondition}) and ${permissionField} contains '${this.userEmail}'`
    }

    return orCondition
  }

  add(
    condition: YqlCondition,
    requirePermissions: boolean = this.requirePermissions,
  ): Or {
    return new Or([...this.conditions, condition], {
      requirePermissions,
      userEmail: this.userEmail,
      permissionType: this.permissionType,
      bypassPermissions: this.bypassPermissions,
    })
  }

  /**
   * Get the conditions (for recursive processing)
   */
  getConditions(): YqlCondition[] {
    return [...this.conditions]
  }

  /**
   * Check if permission have been bypassed explicitly
   */
  isPermissionBypassed(): boolean {
    return this.bypassPermissions
  }

  /**
   * Creates an OrCondition with automatic email permission checking (permissions field) - explicit method
   */
  static withEmailPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): Or {
    return new Or(conditions, {
      requirePermissions: true,
      userEmail,
      permissionType: PermissionFieldType.PERMISSIONS,
    })
  }

  /**
   * Creates an OrCondition with automatic owner permission checking (owner field)
   */
  static withOwnerPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): Or {
    return new Or(conditions, {
      requirePermissions: true,
      userEmail,
      permissionType: PermissionFieldType.OWNER,
    })
  }

  /**
   * Creates an OrCondition without any permission checking
   */
  static withoutPermissions(conditions: YqlCondition[]): Or {
    return new Or(conditions, {
      requirePermissions: false,
      bypassPermissions: true,
    })
  }
}

/**
 * NOT condition
 */
export class Not extends BaseCondition {
  constructor(private condition: YqlCondition) {
    super()
  }

  toString(): string {
    return `!(${this.condition.toString()})`
  }
}

/**
 * Parenthesized condition for explicit grouping
 */
export class Parenthesized extends BaseCondition {
  constructor(private condition: YqlCondition) {
    super()
  }

  toString(): string {
    return `(${this.condition.toString()})`
  }
}

/**
 * Timestamp range condition helper
 */
export class Timestamp extends BaseCondition {
  constructor(
    private fromField: FieldName,
    private toField: FieldName,
    private range: TimestampRange,
  ) {
    super()
  }

  toString(): string {
    const conditions: string[] = []

    if (this.range.from !== null && this.range.from !== undefined) {
      conditions.push(
        `${this.fromField} ${Operator.GREATER_THAN_OR_EQUAL} ${this.range.from}`,
      )
    }

    if (this.range.to !== null && this.range.to !== undefined) {
      conditions.push(
        `${this.toField} ${Operator.LESS_THAN_OR_EQUAL} ${this.range.to}`,
      )
    }

    if (conditions.length === 0) {
      throw new Error(
        "Timestamp range condition requires at least one valid timestamp",
      )
    }

    return conditions.join(" and ")
  }
}

/**
 * Exclusion condition for document IDs
 */
export class Exclude extends BaseCondition {
  constructor(private docIds: string[]) {
    super()
  }

  toString(): string {
    if (!this.docIds || this.docIds.length === 0) {
      return ""
    }

    const conditions = this.docIds
      .filter((id) => id && id.trim())
      .map((id) => `docId ${Operator.CONTAINS} '${escapeYqlValue(id.trim())}'`)

    if (conditions.length === 0) {
      return ""
    }

    return `!(${conditions.join(" or ")})`
  }

  isEmpty(): boolean {
    return (
      !this.docIds ||
      this.docIds.length === 0 ||
      this.docIds.every((id) => !id || !id.trim())
    )
  }
}

/**
 * Inclusion condition for multiple values in a field
 */
export class Include extends BaseCondition {
  constructor(
    private field: FieldName,
    private values: string[],
  ) {
    super()
  }

  toString(): string {
    if (!this.values || this.values.length === 0) {
      return ""
    }

    const conditions = this.values
      .filter((value) => value && value.trim())
      .map(
        (value) =>
          `${this.field} ${Operator.CONTAINS} '${escapeYqlValue(value.trim())}'`,
      )

    if (conditions.length === 0) {
      return ""
    }

    return conditions.length === 1
      ? conditions[0]!
      : `(${conditions.join(" or ")})`
  }

  isEmpty(): boolean {
    return (
      !this.values ||
      this.values.length === 0 ||
      this.values.every((value) => !value || !value.trim())
    )
  }
}

export class Raw extends BaseCondition {
  constructor(private condition: string) {
    super()
  }
  toString(): string {
    return this.condition
  }
}
