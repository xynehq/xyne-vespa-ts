import { SearchModes } from "../types"

export interface YqlCondition {
  readonly __brand: "YqlCondition"
  toString(): string
}

export interface YqlClause {
  toString(): string
}

export interface TimestampRange {
  from?: number | null
  to?: number | null
}

export interface PermissionFilter {
  email?: string
  permissions?: string[]
  owner?: string
}

export interface YqlProfile {
  profile: SearchModes
  yql: string
}

export interface YqlBuilderOptions {
  email?: string
  sources?: string[]
  targetHits?: number
  limit?: number
  offset?: number
  timeout?: string
  requirePermissions: boolean
  validateSyntax?: boolean
}

export type FieldName = string
export type FieldValue = string | number | boolean | Object
export type LogicalOperator = "and" | "or" | "not"
export enum Operator {
  CONTAINS = "contains",
  MATCHES = "matches",
  IN = "in",
  GREATER_THAN_OR_EQUAL = ">=",
  LESS_THAN_OR_EQUAL = "<=",
  EQUAL = "=",
  SUBSTRING_MATCH = "=~",
  GREATER_THAN = ">",
  LESS_THAN = "<",
}

export interface SearchCondition {
  field: FieldName
  operator: Operator
  value: FieldValue
}

export interface NearestNeighborCondition {
  field: FieldName
  queryParam: string
  targetHits?: number
}

export interface UserInputCondition {
  queryParam: string
  targetHits?: number
}

export enum PermissionFieldType {
  OWNER = "owner",
  PERMISSIONS = "permissions",
  BOTH = "both",
}

export interface PermissionOptions {
  requirePermissions?: boolean
  userEmail?: string
  permissionType?: PermissionFieldType
  bypassPermissions?: boolean
}
