import { Type } from '@fastify/type-provider-typebox'

export const SinglePageViewSchema = Type.Object({
  page: Type.String(),
  timestamp: Type.String()
})

export const MultiPageViewSchema = Type.Record(
  Type.String(),
  Type.Record(
    Type.String(),
    Type.Number()
  )
)

export const ReportResponseSchema = Type.Object({
  data: Type.Array(Type.Object({
    h: Type.Number(),
    v: Type.Number()
  }))
})

export type SinglePageView = typeof SinglePageViewSchema.static
export type MultiPageView = typeof MultiPageViewSchema.static
export type ReportResponse = typeof ReportResponseSchema.static