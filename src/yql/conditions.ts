import { PermissionCondition } from "./permissions"
import {
  YqlCondition,
  LogicalOperator,
  SearchCondition,
  TimestampRange,
  FieldName,
  FieldValue,
  PermissionFieldType,
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
  abstract toString(): string

  and(other: YqlCondition): AndCondition {
    return new AndCondition([this, other])
  }

  or(other: YqlCondition): OrCondition {
    return new OrCondition([this, other])
  }

  not(): NotCondition {
    return new NotCondition(this)
  }

  parenthesize(): ParenthesizedCondition {
    return new ParenthesizedCondition(this)
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
    return new VespaField(field, "contains", value)
  }

  static equals(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, "=", value)
  }

  static greaterThan(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, ">", value)
  }

  static greaterThanOrEqual(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, ">=", value)
  }

  static lessThan(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, "<", value)
  }

  static lessThanOrEqual(field: FieldName, value: FieldValue): VespaField {
    return new VespaField(field, "<=", value)
  }
}

/**
 * User input condition for text search
 */
export class UserInputCondition extends BaseCondition {
  constructor(
    private queryParam: string = "@query",
    private targetHits?: number,
  ) {
    super()
  }

  toString(): string {
    const hitsPrefix = this.targetHits ? `{targetHits:${this.targetHits}} ` : ""
    return `(${hitsPrefix}userInput(${this.queryParam}))`
  }
}

/**
 * Nearest neighbor condition for vector search
 */
export class NearestNeighborCondition extends BaseCondition {
  constructor(
    private field: FieldName,
    private queryParam: string = "e",
    private targetHits?: number,
  ) {
    super()
  }

  toString(): string {
    const hitsPrefix = this.targetHits ? `{targetHits:${this.targetHits}} ` : ""
    return `(${hitsPrefix}nearestNeighbor(${this.field}, ${this.queryParam}))`
  }
}

/**
 * Logical AND condition with automatic email permission checking by default
 */
export class AndCondition extends BaseCondition {
  constructor(
    private conditions: YqlCondition[],
    private requirePermissions: boolean = false,
    private userEmail: string = "@email",
    private permissionType:
      | PermissionFieldType
      | PermissionFieldType.OWNER = PermissionFieldType.PERMISSIONS,
  ) {
    super()
    if (conditions.length === 0) {
      throw new Error("AND condition requires at least one condition")
    }
  }

  toString(): string {
    const andCondition = this.conditions.map((c) => c.toString()).join(" and ")

    if (this.requirePermissions) {
      const permissionField =
        this.permissionType === PermissionFieldType.OWNER
          ? PermissionFieldType.OWNER
          : PermissionFieldType.PERMISSIONS
      return `${andCondition} and ${permissionField} contains ${this.userEmail}`
    }

    return andCondition
  }

  add(condition: YqlCondition): AndCondition {
    return new AndCondition(
      [...this.conditions, condition],
      this.requirePermissions,
      this.userEmail,
      this.permissionType,
    )
  }

  /**
   * Creates an AndCondition with automatic email permission checking (permissions field) - explicit method
   */
  static withEmailPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): AndCondition {
    return new AndCondition(
      conditions,
      true,
      userEmail,
      PermissionFieldType.PERMISSIONS,
    )
  }

  /**
   * Creates an AndCondition with automatic owner permission checking (owner field)
   */
  static withOwnerPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): AndCondition {
    return new AndCondition(
      conditions,
      true,
      userEmail,
      PermissionFieldType.OWNER,
    )
  }

  /**
   * Creates an AndCondition without any permission checking
   */
  static withoutPermissions(conditions: YqlCondition[]): AndCondition {
    return new AndCondition(conditions, false)
  }
}

/**
 * Logical OR condition with automatic email permission checking by default
 */
export class OrCondition extends BaseCondition {
  constructor(
    private conditions: YqlCondition[],
    private requirePermissions: boolean = true, // Default to true for email permissions
    private userEmail: string = "@email",
    private permissionType: PermissionFieldType = PermissionFieldType.BOTH,
  ) {
    super()
    if (conditions.length === 0) {
      throw new Error("OR condition requires at least one condition")
    }
  }

  toString(): string {
    const orCondition = this.conditions.map((c) => c.toString()).join(" or ")

    if (this.requirePermissions) {
      if (this.permissionType === PermissionFieldType.BOTH) {
        return `(${orCondition}) and (
                ${PermissionFieldType.OWNER} contains ${this.userEmail} or 
                ${PermissionFieldType.PERMISSIONS} contains ${this.userEmail}
                )`
      }
      const permissionField =
        this.permissionType === PermissionFieldType.OWNER
          ? PermissionFieldType.OWNER
          : PermissionFieldType.PERMISSIONS
      return `(${orCondition}) and ${permissionField} contains ${this.userEmail}`
    }

    return orCondition
  }

  add(
    condition: YqlCondition,
    requirePermissions: boolean = this.requirePermissions,
  ): OrCondition {
    return new OrCondition(
      [...this.conditions, condition],
      requirePermissions,
      this.userEmail,
      this.permissionType,
    )
  }

  /**
   * Creates an OrCondition with automatic email permission checking (permissions field) - explicit method
   */
  static withEmailPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): OrCondition {
    return new OrCondition(
      conditions,
      true,
      userEmail,
      PermissionFieldType.PERMISSIONS,
    )
  }

  /**
   * Creates an OrCondition with automatic owner permission checking (owner field)
   */
  static withOwnerPermissions(
    conditions: YqlCondition[],
    userEmail: string = "@email",
  ): OrCondition {
    return new OrCondition(
      conditions,
      true,
      userEmail,
      PermissionFieldType.OWNER,
    )
  }

  /**
   * Creates an OrCondition without any permission checking
   */
  static withoutPermissions(conditions: YqlCondition[]): OrCondition {
    return new OrCondition(conditions, false)
  }
}

/**
 * NOT condition
 */
export class NotCondition extends BaseCondition {
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
export class ParenthesizedCondition extends BaseCondition {
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
export class TimestampCondition extends BaseCondition {
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
      conditions.push(`${this.fromField} >= ${this.range.from}`)
    }

    if (this.range.to !== null && this.range.to !== undefined) {
      conditions.push(`${this.toField} <= ${this.range.to}`)
    }

    if (conditions.length === 0) {
      throw new Error("Timestamp range must have at least from or to value")
    }

    return conditions.join(" and ")
  }
}

/**
 * Exclusion condition for document IDs
 */
export class ExclusionCondition extends BaseCondition {
  constructor(private docIds: string[]) {
    super()
  }

  toString(): string {
    if (!this.docIds || this.docIds.length === 0) {
      return ""
    }

    const conditions = this.docIds
      .filter((id) => id && id.trim())
      .map((id) => `docId contains '${escapeYqlValue(id.trim())}'`)

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
export class InclusionCondition extends BaseCondition {
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
        (value) => `${this.field} contains '${escapeYqlValue(value.trim())}'`,
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

export class RawCondition extends BaseCondition {
  constructor(private condition: string) {
    super()
  }
  toString(): string {
    return this.condition
  }
}
