import { int } from "zod"
import type { ILogger, MailParticipant } from "../types"
import { YqlCondition } from "../yql/types"
import { contains, or } from "../yql"

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
export const processGmailIntent = (
  mailParticipants: MailParticipant,
  logger: ILogger,
): YqlCondition[] => {
  const mailParticipantsConditions: YqlCondition[] = []

  // Helper function to validate email addresses
  const isValidEmailAddress = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    return emailRegex.test(email)
  }

  // VALIDATION: Process mailParticipants if there are actual email addresses OR subject fields
  // DO NOT process mailParticipants for names without email addresses (unless subject is present)
  const hasValidEmailAddresses =
    mailParticipants &&
    ((mailParticipants.from &&
      mailParticipants.from.length > 0 &&
      mailParticipants.from.some(isValidEmailAddress)) ||
      (mailParticipants.to &&
        mailParticipants.to.length > 0 &&
        mailParticipants.to.some(isValidEmailAddress)) ||
      (mailParticipants.cc &&
        mailParticipants.cc.length > 0 &&
        mailParticipants.cc.some(isValidEmailAddress)) ||
      (mailParticipants.bcc &&
        mailParticipants.bcc.length > 0 &&
        mailParticipants.bcc.some(isValidEmailAddress)))

  const hasSubjectFields =
    mailParticipants &&
    mailParticipants.subject &&
    mailParticipants.subject.length > 0

  // Process intent if we have valid email addresses OR subject fields
  if (!hasValidEmailAddresses && !hasSubjectFields) {
    logger.debug(
      "Mail participants contain only names or no actionable identifiers - skipping Gmail participants filtering",
      { mailParticipants },
    )
    return [] // Return empty array if no valid email addresses or subjects found
  }

  logger.debug(
    "Mail participants contain valid email addresses or subjects - processing Gmail participants filtering",
    { mailParticipants },
  )

  // Process 'from' field
  if (mailParticipants.from && mailParticipants.from.length > 0) {
    if (mailParticipants.from.length === 1 && mailParticipants.from[0]) {
      const fromCondition = contains(
        `\"from\"`,
        `${escapeYqlValue(mailParticipants.from[0])}`,
      )
      mailParticipantsConditions.push(fromCondition)
    } else {
      const fromConditions = mailParticipants.from.map((email) =>
        contains(`\"from\"`, `${escapeYqlValue(email)}`),
      )
      mailParticipantsConditions.push(or(fromConditions))
    }
  }

  // Process 'to' field
  if (
    mailParticipants.to &&
    mailParticipants.to.length > 0 &&
    mailParticipants.to[0]
  ) {
    if (mailParticipants.to.length === 1) {
      const toCondition = contains(
        `\"to\"`,
        `${escapeYqlValue(mailParticipants.to[0])}`,
      )
      mailParticipantsConditions.push(toCondition)
    } else {
      const toConditions = mailParticipants.to.map((email) =>
        contains(`\"to\"`, `${escapeYqlValue(email)}`),
      )
      mailParticipantsConditions.push(or(toConditions))
    }
  }

  // Process 'cc' field
  if (
    mailParticipants.cc &&
    mailParticipants.cc.length > 0 &&
    mailParticipants.cc[0]
  ) {
    if (mailParticipants.cc.length === 1) {
      const ccCondition = contains(
        "cc",
        `${escapeYqlValue(mailParticipants.cc[0])}`,
      )
      mailParticipantsConditions.push(ccCondition)
    } else {
      const ccConditions = mailParticipants.cc.map((email) =>
        contains("cc", `${escapeYqlValue(email)}`),
      )
      mailParticipantsConditions.push(or(ccConditions))
    }
  }

  // Process 'bcc' field
  if (
    mailParticipants.bcc &&
    mailParticipants.bcc.length > 0 &&
    mailParticipants.bcc[0]
  ) {
    if (mailParticipants.bcc.length === 1) {
      const bccCondition = contains(
        "bcc",
        `${escapeYqlValue(mailParticipants.bcc[0])}`,
      )
      mailParticipantsConditions.push(bccCondition)
    } else {
      const bccConditions = mailParticipants.bcc.map((email) =>
        contains("bcc", `${escapeYqlValue(email)}`),
      )
      mailParticipantsConditions.push(or(bccConditions))
    }
  }

  // Process 'subject' field
  if (
    mailParticipants.subject &&
    mailParticipants.subject.length > 0 &&
    mailParticipants.subject[0]
  ) {
    if (mailParticipants.subject.length === 1) {
      const subjectCondition = contains(
        "subject",
        `${escapeYqlValue(mailParticipants.subject[0])}`,
      )
      mailParticipantsConditions.push(subjectCondition)
    } else {
      const subjectConditions = mailParticipants.subject.map((subj) =>
        contains("subject", `${escapeYqlValue(subj)}`),
      )
      mailParticipantsConditions.push(or(subjectConditions))
    }
  }

  return mailParticipantsConditions
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
