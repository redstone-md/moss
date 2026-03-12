import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ActionDeck } from './components/ActionDeck'
import { ArtifactList } from './components/ArtifactList'
import { MessagePanel } from './components/MessagePanel'
import { MilestoneList } from './components/MilestoneList'
import { PeerPanel } from './components/PeerPanel'
import { RoomList } from './components/RoomList'
import { RuntimePanel } from './components/RuntimePanel'
import { desktopStatusClient } from './lib/desktopStatusClient'

export function App() {
  const [selectedRoomId, setSelectedRoomId] = useState('lobby')
  const [roomDraft, setRoomDraft] = useState('release-war-room')
  const [peerDraft, setPeerDraft] = useState('')
  const [messageDraft, setMessageDraft] = useState('')
  const queryClient = useQueryClient()

  const snapshot = useQuery({
    queryKey: ['desktop-snapshot'],
    queryFn: () => desktopStatusClient.getSnapshot(),
    refetchInterval: 1500,
  })

  const toggleRuntime = useMutation({
    mutationFn: () => desktopStatusClient.toggleRuntime(),
    onSuccess: (data) => {
      queryClient.setQueryData(['desktop-snapshot'], data)
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
  const activeRoom =
    data.rooms.find((room) => room.id === selectedRoomId) ?? data.rooms[0]
  const visibleMessages = data.messages.filter(
    (message) => message.roomId === activeRoom.id,
  )
  const visiblePeers = data.peers.filter((peer) =>
    peer.rooms.includes(activeRoom.label),
  )
  const actionError =
    subscribeRoom.error?.message ?? connectPeer.error?.message
  const sendError = publishMessage.error?.message

  return (
    <main className="shell shell-chat">
      <RuntimePanel
        state={data.runtime.state}
        summary={data.runtime.summary}
        route={data.runtime.route}
        natHint={data.runtime.natHint}
        sharedBridge={data.runtime.sharedBridge}
        errorNote={toggleRuntime.isError ? toggleRuntime.error.message : undefined}
        onToggle={() => toggleRuntime.mutate()}
        isBusy={toggleRuntime.isPending}
      />

      <section className="chat-grid">
        <RoomList
          rooms={data.rooms}
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
        <PeerPanel peers={visiblePeers} />
      </section>

      <section className="content-grid">
        <ActionDeck
          appName={data.appName}
          version={data.version}
          branch={data.branch}
          stage={data.stage}
          roomDraft={roomDraft}
          peerDraft={peerDraft}
          onRoomDraftChange={setRoomDraft}
          onPeerDraftChange={setPeerDraft}
          onJoinRoom={() => subscribeRoom.mutate()}
          onConnectPeer={() => connectPeer.mutate()}
          busyAction={subscribeRoom.isPending ? 'join' : connectPeer.isPending ? 'connect' : undefined}
          errorNote={actionError}
        />
        <ArtifactList artifacts={data.artifacts} />
      </section>

      <MilestoneList milestones={data.milestones} />
    </main>
  )
}
