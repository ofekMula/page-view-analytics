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
  page: Type.String(),
  start: Type.String({ format: 'date-time' }),
  end: Type.String({ format: 'date-time' }),
  data: Type.Array(
    Type.Object({
      hour: Type.Number(),
      views: Type.Number(),
    })
  ),
});

export type Aggregation = {
  page: string;
  viewHour: Date;
  views: number;
  partition: number;
  shard_key: number;
};

export type SinglePageView = typeof SinglePageViewSchema.static
export type MultiPageView = typeof MultiPageViewSchema.static
export type ReportResponse = typeof ReportResponseSchema.static
