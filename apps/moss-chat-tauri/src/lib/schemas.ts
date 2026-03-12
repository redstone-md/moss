import { z } from 'zod'

export const artifactSchema = z.object({
  name: z.string().min(1),
  platform: z.string().min(1),
  notes: z.string().min(1),
})

export const runtimeStatusSchema = z.object({
  state: z.string().min(1),
  summary: z.string().min(1),
  route: z.string().min(1),
  natHint: z.string().min(1),
  sharedBridge: z.string().min(1),
})

export const roomSummarySchema = z.object({
  id: z.string().min(1),
  label: z.string().min(1),
  unread: z.number().int().nonnegative(),
  participants: z.number().int().nonnegative(),
  kind: z.string().min(1),
})

export const messageSchema = z.object({
  id: z.string().min(1),
  roomId: z.string().min(1),
  author: z.string().min(1),
  body: z.string().min(1),
  timestamp: z.string().min(1),
  emphasis: z.string().min(1),
})

export const peerSummarySchema = z.object({
  id: z.string().min(1),
  displayName: z.string().min(1),
  route: z.string().min(1),
  latency: z.string().min(1),
  status: z.string().min(1),
  rooms: z.array(z.string().min(1)),
})

export const runtimeSettingsSchema = z.object({
  meshId: z.string().min(1),
  listenPort: z.number().int().min(0).max(65535),
  initialRoom: z.string().min(1),
  startupPeer: z.string(),
  trackerMode: z.enum(['default', 'disabled']),
  lanDiscoveryEnabled: z.boolean(),
  configPreview: z.string().min(1),
})

export const runtimeDiagnosticsSchema = z.object({
  configuredMeshId: z.string().min(1),
  configuredListenPort: z.string().min(1),
  initialRoom: z.string().min(1),
  startupPeer: z.string().min(1),
  trackerMode: z.string().min(1),
  lanDiscovery: z.string().min(1),
  activeMeshId: z.string().min(1),
  activeListenPort: z.string().min(1),
  peerCount: z.number().int().nonnegative(),
  channelCount: z.number().int().nonnegative(),
  activeChannels: z.array(z.string().min(1)),
  supernodeReady: z.boolean(),
})

export const updateRuntimeSettingsInputSchema = z.object({
  meshId: z
    .string()
    .trim()
    .min(1)
    .max(64)
    .regex(/^[a-zA-Z0-9._-]+$/, 'Use letters, numbers, dot, dash, or underscore'),
  listenPort: z.coerce.number().int().min(0).max(65535),
  initialRoom: z
    .string()
    .trim()
    .min(1)
    .max(64)
    .regex(/^[a-zA-Z0-9._-]+$/, 'Use letters, numbers, dot, dash, or underscore')
    .transform((value) => value.replace(/^#/, '').toLowerCase()),
  startupPeer: z
    .string()
    .trim()
    .transform((value) => value)
    .refine((value) => value === '' || /^[^:\s]+:\d+$/.test(value), 'Use HOST:PORT'),
  trackerMode: z.enum(['default', 'disabled']),
  lanDiscoveryEnabled: z.boolean(),
})

export const subscribeRoomInputSchema = z.object({
  room: z
    .string()
    .trim()
    .min(1)
    .transform((value) => value.replace(/^#/, '').toLowerCase()),
})

export const connectPeerInputSchema = z.object({
  addr: z
    .string()
    .trim()
    .min(3)
    .regex(/^[^:\s]+:\d+$/, 'Use HOST:PORT'),
})

export const publishMessageInputSchema = z.object({
  room: z
    .string()
    .trim()
    .min(1)
    .transform((value) => value.replace(/^#/, '').toLowerCase()),
  body: z.string().trim().min(1).max(65535),
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
  runtime: runtimeStatusSchema,
  settings: runtimeSettingsSchema,
  diagnostics: runtimeDiagnosticsSchema,
  rooms: z.array(roomSummarySchema),
  messages: z.array(messageSchema),
  peers: z.array(peerSummarySchema),
})

export type Artifact = z.infer<typeof artifactSchema>
export type RuntimeStatus = z.infer<typeof runtimeStatusSchema>
export type RuntimeSettings = z.infer<typeof runtimeSettingsSchema>
export type RuntimeDiagnostics = z.infer<typeof runtimeDiagnosticsSchema>
export type RoomSummary = z.infer<typeof roomSummarySchema>
export type Message = z.infer<typeof messageSchema>
export type PeerSummary = z.infer<typeof peerSummarySchema>
export type Milestone = z.infer<typeof milestoneSchema>
export type DesktopSnapshot = z.infer<typeof desktopSnapshotSchema>
export type UpdateRuntimeSettingsInput = z.infer<typeof updateRuntimeSettingsInputSchema>
export type SubscribeRoomInput = z.infer<typeof subscribeRoomInputSchema>
export type ConnectPeerInput = z.infer<typeof connectPeerInputSchema>
export type PublishMessageInput = z.infer<typeof publishMessageInputSchema>
