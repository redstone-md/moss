import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ActionDeck } from './components/ActionDeck'
import { DiagnosticsPanel } from './components/DiagnosticsPanel'
import { MessagePanel } from './components/MessagePanel'
import { PeerPanel } from './components/PeerPanel'
import { RoomList } from './components/RoomList'
import { RuntimePanel } from './components/RuntimePanel'
import { RuntimeSetupPanel } from './components/RuntimeSetupPanel'
import { useDesktopErrorDialogs } from './hooks/useDesktopErrorDialogs'
import { useDesktopNotifications } from './hooks/useDesktopNotifications'
import { desktopStatusClient } from './lib/desktopStatusClient'
import { getFallbackRoom } from './lib/fallbacks'

export function App() {
  const [selectedRoomId, setSelectedRoomId] = useState('lobby')
  const [nicknameDraft, setNicknameDraft] = useState<string | null>(null)
  const [meshDraft, setMeshDraft] = useState<string | null>(null)
  const [listenPortDraft, setListenPortDraft] = useState<string | null>(null)
  const [initialRoomDraft, setInitialRoomDraft] = useState<string | null>(null)
  const [startupPeerDraft, setStartupPeerDraft] = useState<string | null>(null)
  const [trackerModeDraft, setTrackerModeDraft] = useState<'default' | 'disabled' | null>(
    null,
  )
  const [lanDiscoveryDraft, setLanDiscoveryDraft] = useState<boolean | null>(null)
  const [roomDraft, setRoomDraft] = useState('release-war-room')
  const [peerDraft, setPeerDraft] = useState('')
  const [directDraft, setDirectDraft] = useState('')
  const [messageDraft, setMessageDraft] = useState('')
  const queryClient = useQueryClient()

  const snapshot = useQuery({
    queryKey: ['desktop-snapshot'],
    queryFn: () => desktopStatusClient.getSnapshot(),
    refetchInterval: 1500,
  })

  useDesktopNotifications({
    snapshot: snapshot.data,
    selectedRoomId,
  })

  const toggleRuntime = useMutation({
    mutationFn: () => desktopStatusClient.toggleRuntime(),
    onSuccess: (data) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
    },
  })

  const updateRuntimeSettings = useMutation({
    mutationFn: () =>
      desktopStatusClient.updateRuntimeSettings({
        nickname: nicknameDraft ?? snapshot.data?.settings.nickname ?? 'operator',
        meshId: meshDraft ?? snapshot.data?.settings.meshId ?? 'moss-chat-dev',
        listenPort: Number(listenPortDraft ?? snapshot.data?.settings.listenPort ?? 0),
        initialRoom:
          initialRoomDraft ?? snapshot.data?.settings.initialRoom ?? 'lobby',
        startupPeer:
          startupPeerDraft ?? snapshot.data?.settings.startupPeer ?? '',
        trackerMode:
          trackerModeDraft ?? snapshot.data?.settings.trackerMode ?? 'default',
        lanDiscoveryEnabled:
          lanDiscoveryDraft ?? snapshot.data?.settings.lanDiscoveryEnabled ?? true,
      }),
    onSuccess: (data) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
      setSelectedRoomId(data.settings.initialRoom)
    },
  })

  const subscribeRoom = useMutation({
    mutationFn: () => desktopStatusClient.subscribeRoom({ room: roomDraft }),
    onSuccess: (data) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
      setSelectedRoomId(roomDraft.replace(/^#/, '').toLowerCase())
    },
  })

  const connectPeer = useMutation({
    mutationFn: () => desktopStatusClient.connectPeer({ addr: peerDraft }),
    onSuccess: (data) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
      setPeerDraft('')
    },
  })

  const openDirectRoom = useMutation({
    mutationFn: (target?: string) =>
      desktopStatusClient.openDirectRoom({ target: target ?? directDraft }),
    onSuccess: (data, target) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
      const targetLabel = (target ?? directDraft).trim().toLowerCase()
      const directRoom =
        data.rooms.find((room) => room.label.toLowerCase() === `@${targetLabel}`) ??
        data.rooms.find((room) => room.kind === 'dm')
      if (directRoom) {
        setSelectedRoomId(directRoom.id)
      }
      setDirectDraft('')
    },
  })

  const publishMessage = useMutation({
    mutationFn: () =>
      desktopStatusClient.publishMessage({
        room: selectedRoomId,
        body: messageDraft,
      }),
    onSuccess: (updatedSnapshot) => {
      queryClient.setQueryData(['desktop-snapshot'], updatedSnapshot)
      setMessageDraft('')
    },
  })

  const settingsError = updateRuntimeSettings.error?.message
  const actionError =
    subscribeRoom.error?.message ??
    connectPeer.error?.message ??
    openDirectRoom.error?.message
  const sendError = publishMessage.error?.message
  const runtimeError = toggleRuntime.error?.message

  useDesktopErrorDialogs({
    errors: [settingsError, actionError, sendError, runtimeError].filter(
      (value): value is string => Boolean(value),
    ),
  })

  if (snapshot.isPending) {
    return <main className="shell loading">Loading desktop runtime snapshot...</main>
  }

  if (snapshot.isError) {
    return (
      <main className="shell loading">
        <section className="error-panel">
          <p className="eyebrow">Bootstrap error</p>
          <h1>Desktop shell did not start cleanly</h1>
          <p>{snapshot.error.message}</p>
        </section>
      </main>
    )
  }

  const data = snapshot.data
  const settings = data.settings
  const rooms = data.rooms.length > 0 ? data.rooms : [getFallbackRoom()]
  const nicknameValue = nicknameDraft ?? settings.nickname
  const meshValue = meshDraft ?? settings.meshId
  const listenPortValue = listenPortDraft ?? `${settings.listenPort}`
  const initialRoomValue = initialRoomDraft ?? settings.initialRoom
  const startupPeerValue = startupPeerDraft ?? settings.startupPeer
  const trackerModeValue = trackerModeDraft ?? settings.trackerMode
  const lanDiscoveryValue = lanDiscoveryDraft ?? settings.lanDiscoveryEnabled
  const activeRoom =
    rooms.find((room) => room.id === selectedRoomId) ??
    rooms.find((room) => room.id === settings.initialRoom) ??
    rooms[0]
  const visibleMessages = data.messages.filter(
    (message) => message.roomId === activeRoom.id,
  )
  const visiblePeers = data.peers.filter((peer) =>
    activeRoom.kind === 'system'
      ? true
      : peer.rooms.includes(activeRoom.label) ||
        peer.rooms.includes(`#${activeRoom.id}`),
  )

  return (
    <main className="shell shell-chat">
      <RuntimePanel
        state={data.runtime.state}
        summary={data.runtime.summary}
        route={data.runtime.route}
        natHint={data.runtime.natHint}
        sharedBridge={data.runtime.sharedBridge}
        isOnline={data.runtime.state === 'Runtime online'}
        errorNote={runtimeError}
        onToggle={() => toggleRuntime.mutate()}
        isBusy={toggleRuntime.isPending}
      />

      <section className="chat-grid">
        <RoomList
          rooms={rooms}
          selectedRoomId={activeRoom.id}
          onSelect={setSelectedRoomId}
        />
        <MessagePanel
          room={activeRoom}
          messages={visibleMessages}
          draft={messageDraft}
          onDraftChange={setMessageDraft}
          onSend={() => publishMessage.mutate()}
          isSending={publishMessage.isPending}
          errorNote={sendError}
        />
        <PeerPanel
          peers={visiblePeers}
          onOpenDirectRoom={(target) => {
            setDirectDraft(target)
            openDirectRoom.mutate(target)
          }}
        />
      </section>

      <section className="content-grid">
        <RuntimeSetupPanel
          nickname={nicknameValue}
          meshId={meshValue}
          listenPort={listenPortValue}
          initialRoom={initialRoomValue}
          startupPeer={startupPeerValue}
          trackerMode={trackerModeValue}
          lanDiscoveryEnabled={lanDiscoveryValue}
          configPreview={settings.configPreview}
          errorNote={settingsError}
          isSaving={updateRuntimeSettings.isPending}
          onNicknameChange={setNicknameDraft}
          onMeshIdChange={setMeshDraft}
          onListenPortChange={setListenPortDraft}
          onInitialRoomChange={setInitialRoomDraft}
          onStartupPeerChange={setStartupPeerDraft}
          onTrackerModeChange={setTrackerModeDraft}
          onLanDiscoveryChange={setLanDiscoveryDraft}
          onSave={() => updateRuntimeSettings.mutate()}
        />
        <DiagnosticsPanel diagnostics={data.diagnostics} />
      </section>

      <section className="content-grid content-grid-actions">
        <ActionDeck
          appName={data.appName}
          version={data.version}
          branch={data.branch}
          stage={data.stage}
          roomDraft={roomDraft}
          peerDraft={peerDraft}
          directDraft={directDraft}
          onRoomDraftChange={setRoomDraft}
          onPeerDraftChange={setPeerDraft}
          onDirectDraftChange={setDirectDraft}
          onJoinRoom={() => subscribeRoom.mutate()}
          onConnectPeer={() => connectPeer.mutate()}
          onOpenDirectRoom={() => openDirectRoom.mutate(directDraft)}
          busyAction={
            subscribeRoom.isPending
              ? 'join'
              : connectPeer.isPending
                ? 'connect'
                : openDirectRoom.isPending
                  ? 'dm'
                  : undefined
          }
          errorNote={actionError}
        />
        <div className="panel action-context">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Channel context</p>
              <h2>{activeRoom.label}</h2>
            </div>
          </div>
          <div className="hero-meta hero-meta-left">
            <span>{data.branch}</span>
            <span>{data.stage}</span>
            <span>{visiblePeers.length} visible peers</span>
          </div>
          <p className="runtime-summary">
            Publish, room subscribe, and direct connect are already going through the
            shared Moss runtime. Runtime settings above apply on the next start.
          </p>
        </div>
      </section>
    </main>
  )
}
