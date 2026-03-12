import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ActionDeck } from './components/ActionDeck'
import { ChatHeader } from './components/ChatHeader'
import { DiagnosticsPanel } from './components/DiagnosticsPanel'
import { MessagePanel } from './components/MessagePanel'
import { OnboardingScreen } from './components/OnboardingScreen'
import { PeerPanel } from './components/PeerPanel'
import { ProfileEditorPanel } from './components/ProfileEditorPanel'
import { RuntimePanel } from './components/RuntimePanel'
import { Sidebar } from './components/Sidebar'
import { useDesktopErrorDialogs } from './hooks/useDesktopErrorDialogs'
import { useDesktopNotifications } from './hooks/useDesktopNotifications'
import { desktopStatusClient } from './lib/desktopStatusClient'
import { getFallbackRoom } from './lib/fallbacks'

type ShellView = 'chat' | 'profile'

export function App() {
  const [selectedRoomId, setSelectedRoomId] = useState('lobby')
  const [selectedView, setSelectedView] = useState<ShellView>('chat')
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [onboardingDismissed, setOnboardingDismissed] = useState(false)
  const [createMode, setCreateMode] = useState(false)
  const [roomSearch, setRoomSearch] = useState('')
  const [nicknameDraft, setNicknameDraft] = useState<string | null>(null)
  const [meshDraft, setMeshDraft] = useState<string | null>(null)
  const [listenPortDraft, setListenPortDraft] = useState<string | null>(null)
  const [initialRoomDraft, setInitialRoomDraft] = useState<string | null>(null)
  const [startupPeerDraft, setStartupPeerDraft] = useState<string | null>(null)
  const [trackerModeDraft, setTrackerModeDraft] = useState<'default' | 'disabled' | null>(
    null,
  )
  const [lanDiscoveryDraft, setLanDiscoveryDraft] = useState<boolean | null>(null)
  const [roomDraft, setRoomDraft] = useState('design-reviews')
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
        initialRoom: initialRoomDraft ?? snapshot.data?.settings.initialRoom ?? 'lobby',
        startupPeer: startupPeerDraft ?? snapshot.data?.settings.startupPeer ?? '',
        trackerMode: trackerModeDraft ?? snapshot.data?.settings.trackerMode ?? 'default',
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
      const normalizedRoom = roomDraft.replace(/^#/, '').toLowerCase()
      setSelectedRoomId(normalizedRoom)
      setCreateMode(false)
      setSidebarOpen(false)
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
      setSelectedView('chat')
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

  const filteredRooms = useMemo(() => {
    const needle = roomSearch.trim().toLowerCase()
    if (!needle) {
      return rooms
    }
    return rooms.filter((room) => room.label.toLowerCase().includes(needle))
  }, [roomSearch, rooms])

  const activeRoom =
    rooms.find((room) => room.id === selectedRoomId) ??
    rooms.find((room) => room.id === settings.initialRoom) ??
    rooms[0]

  const visibleMessages = useMemo(
    () => data.messages.filter((message) => message.roomId === activeRoom.id),
    [activeRoom.id, data.messages],
  )

  const visiblePeers = useMemo(
    () =>
      data.peers.filter((peer) =>
        activeRoom.kind === 'system'
          ? true
          : peer.rooms.includes(activeRoom.label) || peer.rooms.includes(`#${activeRoom.id}`),
      ),
    [activeRoom.id, activeRoom.kind, activeRoom.label, data.peers],
  )

  async function applyAndStartRuntime() {
    const updatedSnapshot = await updateRuntimeSettings.mutateAsync()
    setSelectedRoomId(updatedSnapshot.settings.initialRoom)
    if (updatedSnapshot.runtime.state !== 'Runtime online') {
      const runningSnapshot = await toggleRuntime.mutateAsync()
      setSelectedRoomId(runningSnapshot.settings.initialRoom)
    }
    setOnboardingDismissed(true)
  }

  const showOnboarding = data.runtime.state !== 'Runtime online' && !onboardingDismissed

  if (showOnboarding) {
    return (
      <OnboardingScreen
        nickname={nicknameValue}
        meshId={meshValue}
        listenPort={listenPortValue}
        initialRoom={initialRoomValue}
        startupPeer={startupPeerValue}
        trackerMode={trackerModeValue}
        lanDiscoveryEnabled={lanDiscoveryValue}
        configPreview={settings.configPreview}
        errorNote={settingsError ?? runtimeError}
        isSaving={updateRuntimeSettings.isPending || toggleRuntime.isPending}
        onNicknameChange={setNicknameDraft}
        onMeshIdChange={setMeshDraft}
        onListenPortChange={setListenPortDraft}
        onInitialRoomChange={setInitialRoomDraft}
        onStartupPeerChange={setStartupPeerDraft}
        onTrackerModeChange={setTrackerModeDraft}
        onLanDiscoveryChange={setLanDiscoveryDraft}
        onSave={() => void applyAndStartRuntime()}
        onSkip={() => setOnboardingDismissed(true)}
      />
    )
  }

  return (
    <main className="shell shell-chat shell-app">
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

      <section className="workspace-shell">
        <div className={`sidebar-drawer${sidebarOpen ? ' sidebar-drawer-open' : ''}`}>
          <Sidebar
            rooms={filteredRooms}
            selectedRoomId={activeRoom.id}
            roomSearch={roomSearch}
            roomDraft={roomDraft}
            createMode={createMode}
            onRoomSearchChange={setRoomSearch}
            onRoomDraftChange={setRoomDraft}
            onToggleCreateMode={() => setCreateMode((current) => !current)}
            onCreateRoom={() => subscribeRoom.mutate()}
            onSelectRoom={(roomId) => {
              setSelectedRoomId(roomId)
              setSelectedView('chat')
              setSidebarOpen(false)
            }}
            onOpenProfile={() => {
              setSelectedView('profile')
              setSidebarOpen(false)
            }}
          />
        </div>

        <section className="chat-pane">
          <ChatHeader
            room={activeRoom}
            peers={visiblePeers}
            runtime={data.runtime}
            onToggleSidebar={() => setSidebarOpen((current) => !current)}
          />

          {selectedView === 'profile' ? (
            <div className="chat-content profile-content">
              <ProfileEditorPanel
                nickname={nicknameValue}
                meshId={meshValue}
                initialRoom={initialRoomValue}
                startupPeer={startupPeerValue}
                listenPort={listenPortValue}
                trackerMode={trackerModeValue}
                lanDiscoveryEnabled={lanDiscoveryValue}
                configPreview={settings.configPreview}
                errorNote={settingsError}
                isSaving={updateRuntimeSettings.isPending}
                onNicknameChange={setNicknameDraft}
                onMeshIdChange={setMeshDraft}
                onInitialRoomChange={setInitialRoomDraft}
                onStartupPeerChange={setStartupPeerDraft}
                onListenPortChange={setListenPortDraft}
                onTrackerModeChange={setTrackerModeDraft}
                onLanDiscoveryChange={setLanDiscoveryDraft}
                onSave={() => updateRuntimeSettings.mutate()}
              />
              <DiagnosticsPanel diagnostics={data.diagnostics} />
            </div>
          ) : (
            <div className="chat-content">
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
            </div>
          )}
        </section>
      </section>

      <section className="bottom-dock">
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
      </section>
    </main>
  )
}
