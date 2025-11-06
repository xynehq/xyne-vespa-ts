import { int } from "zod"
import { Apps, ILogger, MailParticipant, VespaSearchResponse } from "../types"
import { YqlCondition } from "../yql/types"
import { contains, matches, or } from "../yql"

export function scale(val: number): number | null {
  if (!val) return null
  return (2 * Math.atan(val / 4)) / Math.PI
}

export const getErrorMessage = (error: unknown) => {
  if (error instanceof Error) return error.message
  return String(error)
}

export const escapeYqlValue = (value: string): string => {
  return value.replace(/'/g, "''")
}

export const isValidEmail = (email: string) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
export const getGmailParticipantsConditions = (
  mailParticipants: MailParticipant,
  logger: ILogger,
): YqlCondition[] => {
  if (!mailParticipants || Object.keys(mailParticipants).length === 0) {
    logger.info("No mail participants provided for Gmail filtering")
    return []
  }

  const participantFields: Record<keyof MailParticipant, string> = {
    from: `"from"`,
    to: "to",
    cc: "cc",
    bcc: "bcc",
  }

  return (Object.keys(participantFields) as (keyof MailParticipant)[]).flatMap(
    (field) => {
      const queryField = participantFields[field]
      const values = mailParticipants[field] || []
      if (!values.length) return []

      const conditions = values.map((email) => {
        return isValidEmail(email)
          ? contains(queryField, email)
          : matches(queryField, email)
      })

      return or(conditions)
    },
  )
}

export const dateToUnixTimestamp = (
  dateString: string,
  endOfDay: boolean = false,
): string => {
  const date = new Date(dateString)

  if (isNaN(date.getTime())) {
    throw new Error(
      `Invalid date format: ${dateString}. Expected format: YYYY-MM-DD`,
    )
  }

  if (endOfDay) {
    date.setHours(23, 59, 59, 999)
  } else {
    date.setHours(0, 0, 0, 0)
  }

  const timestampMs = date.getTime()

  const seconds = Math.floor(timestampMs / 1000)
  const microseconds = (timestampMs % 1000) * 1000

  return `${seconds}.${microseconds.toString().padStart(6, "0")}`
}

export const formatYqlToReadable = (yql: string) => {
  const lines = yql
    .trim()
    // Normalize operators to have consistent spacing
    .replace(/\s+(or|and)\s+/gi, " $1 ")
    // Add line breaks before logical operators
    .replace(/\s+(OR|or)\s+/gi, "\n OR ")
    .replace(/\s+(AND|and)\s+/gi, "\n AND ")
    // Handle parentheses - add breaks after opening and before closing
    .replace(/\(/g, "(\n")
    .replace(/\)/g, "\n)")
    .split("\n")
    .filter((line) => line.trim() !== "") // Remove empty lines

  let indentLevel = 0
  const indentSize = 2

  return lines
    .map((line) => {
      const trimmed = line.trim()

      // Decrease indent for closing parentheses
      if (trimmed.startsWith(")")) {
        indentLevel = Math.max(0, indentLevel - 1)
      }

      const indentedLine = " ".repeat(indentLevel * indentSize) + trimmed

      // Increase indent for opening parentheses
      if (trimmed.endsWith("(")) {
        indentLevel++
      }

      return indentedLine
    })
    .join("\n")
}

export function isValidTimestampRange(
  range: { from: number | null; to: number | null } | null,
): range is { from: number | null; to: number | null } {
  return (
    !!range && (typeof range.from === "number" || typeof range.to === "number")
  )
}

export const normalizeTimestamp = (timestamp: number, app?: Apps): number => {
  const timestampStr = timestamp.toString()
  if (timestampStr.length === 10) {
    // Convert seconds to milliseconds
    if (app == Apps.Slack) {
      return timestamp
    }
    return timestamp * 1000
  } else if (timestampStr.length === 13) {
    // Already in milliseconds
    if (app == Apps.Slack) {
      return timestamp / 1000
    }
    return timestamp
  }
  // For other lengths, assume it's already correct
  return timestamp
}

export const vespaEmptyResponse = (): VespaSearchResponse => ({
  root: {
    id: "empty_ID",
    relevance: 0,
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
})
