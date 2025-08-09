import { int } from "zod"
import type { ILogger } from "../types"
import type { Intent } from "../types"

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



// Gmail intent processing function
export const processGmailIntent = (intent: Intent, logger: ILogger): string[] => {
  const intentConditions: string[] = []

  // Helper function to validate email addresses
  const isValidEmailAddress = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    return emailRegex.test(email)
  }

  // VALIDATION: Process intent if there are actual email addresses OR subject fields
  // DO NOT process intent for names without email addresses (unless subject is present)
  const hasValidEmailAddresses =
    intent &&
    ((intent.from &&
      intent.from.length > 0 &&
      intent.from.some(isValidEmailAddress)) ||
      (intent.to &&
        intent.to.length > 0 &&
        intent.to.some(isValidEmailAddress)) ||
      (intent.cc &&
        intent.cc.length > 0 &&
        intent.cc.some(isValidEmailAddress)) ||
      (intent.bcc &&
        intent.bcc.length > 0 &&
        intent.bcc.some(isValidEmailAddress)))

  const hasSubjectFields = intent && intent.subject && intent.subject.length > 0

  // Process intent if we have valid email addresses OR subject fields
  if (!hasValidEmailAddresses && !hasSubjectFields) {
    logger.debug(
      "Intent contains only names or no actionable identifiers - skipping Gmail intent filtering",
      { intent },
    )
    return [] // Return empty array if no valid email addresses or subjects found
  }

  logger.debug(
    "Intent contains valid email addresses or subjects - processing Gmail intent filtering",
    { intent },
  )

  // Process 'from' field
  if (intent.from && intent.from.length > 0) {
    if (intent.from.length === 1 && intent.from[0]) {
      const fromCondition = `"from" contains '${escapeYqlValue(intent.from[0])}'`
      intentConditions.push(fromCondition)
    } else {
      const fromConditions = intent.from
        .map((email) => `"from" contains '${escapeYqlValue(email)}'`)
        .join(" or ")
      intentConditions.push(`(${fromConditions})`)
    }
  }

  // Process 'to' field
  if (intent.to && intent.to.length > 0 && intent.to[0]) {
    if (intent.to.length === 1) {
      const toCondition = `"to" contains '${escapeYqlValue(intent.to[0])}'`
      intentConditions.push(toCondition)
    } else {
      const toConditions = intent.to
        .map((email) => `"to" contains '${escapeYqlValue(email)}'`)
        .join(" or ")
      intentConditions.push(`(${toConditions})`)
    }
  }

  // Process 'cc' field
  if (intent.cc && intent.cc.length > 0 && intent.cc[0]) {
    if (intent.cc.length === 1) {
      const ccCondition = `cc contains '${escapeYqlValue(intent.cc[0])}'`
      intentConditions.push(ccCondition)
    } else {
      const ccConditions = intent.cc
        .map((email) => `cc contains '${escapeYqlValue(email)}'`)
        .join(" or ")
      intentConditions.push(`(${ccConditions})`)
    }
  }

  // Process 'bcc' field
  if (intent.bcc && intent.bcc.length > 0 && intent.bcc[0]) {
    if (intent.bcc.length === 1) {
      const bccCondition = `bcc contains '${escapeYqlValue(intent.bcc[0])}'`
      intentConditions.push(bccCondition)
    } else {
      const bccConditions = intent.bcc
        .map((email) => `bcc contains '${escapeYqlValue(email)}'`)
        .join(" or ")
      intentConditions.push(`(${bccConditions})`)
    }
  }

  // Process 'subject' field
  if (intent.subject && intent.subject.length > 0 && intent.subject[0]) {
    if (intent.subject.length === 1) {
      const subjectCondition = `"subject" contains '${escapeYqlValue(intent.subject[0])}'`
      intentConditions.push(subjectCondition)
    } else {
      const subjectConditions = intent.subject
        .map((subj) => `"subject" contains '${escapeYqlValue(subj)}'`)
        .join(" or ")
      intentConditions.push(`(${subjectConditions})`)
    }
  }

  return intentConditions
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