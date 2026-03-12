import { z } from 'zod'

export const artifactSchema = z.object({
  name: z.string().min(1),
  platform: z.string().min(1),
  notes: z.string().min(1),
})

export const milestoneSchema = z.object({
  title: z.string().min(1),
  detail: z.string().min(1),
  status: z.enum(['ready', 'next', 'blocked']),
})

export const desktopSnapshotSchema = z.object({
  appName: z.string().min(1),
  version: z.string().min(1),
  branch: z.string().min(1),
  stage: z.string().min(1),
  summary: z.string().min(1),
  sharedStrategy: z.string().min(1),
  artifacts: z.array(artifactSchema),
  milestones: z.array(milestoneSchema),
})

export type Artifact = z.infer<typeof artifactSchema>
export type Milestone = z.infer<typeof milestoneSchema>
export type DesktopSnapshot = z.infer<typeof desktopSnapshotSchema>
