import {
  And,
  Or,
  Exclude,
  Include,
  NearestNeighbor,
  Not,
  Parenthesized,
  Raw,
  Timestamp,
  UserInput,
  VespaField,
} from "./conditions"
import { FieldName, FieldValue, TimestampRange, YqlCondition } from "./types"

export const and = (
  conditions: YqlCondition[],
  requirePermissions = false,
): And => new And(conditions, requirePermissions)
export const or = (conditions: YqlCondition[]): Or => new Or(conditions)
export const not = (condition: YqlCondition): Not => new Not(condition)
export const parenthesize = (condition: YqlCondition): Parenthesized =>
  new Parenthesized(condition)
export const timestamp = (
  fromField: FieldName,
  toField: FieldName,
  range: TimestampRange,
): Timestamp => new Timestamp(fromField, toField, range)
export const exclude = (docIds: string[]): Exclude => new Exclude(docIds)
export const include = (field: FieldName, values: string[]): Include =>
  new Include(field, values)
export const raw = (condition: string): Raw => new Raw(condition)
export const userInput = (
  queryParam: string = "@query",
  targetHits: number,
): UserInput => new UserInput(queryParam, targetHits)
export const nearestNeighbor = (
  field: FieldName,
  queryParam: string = "e",
  targetHits: number,
): NearestNeighbor => new NearestNeighbor(field, queryParam, targetHits)

export const contains = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.contains(field, value)
export const equals = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.equals(field, value)
export const greaterThan = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.greaterThan(field, value)
export const greaterThanOrEqual = (
  field: FieldName,
  value: FieldValue,
): VespaField => VespaField.greaterThanOrEqual(field, value)
export const lessThan = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.lessThan(field, value)
export const lessThanOrEqual = (
  field: FieldName,
  value: FieldValue,
): VespaField => VespaField.lessThanOrEqual(field, value)
