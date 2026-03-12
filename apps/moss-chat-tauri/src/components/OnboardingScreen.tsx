import { RuntimeSetupPanel } from './RuntimeSetupPanel'

type OnboardingScreenProps = {
  nickname: string
  meshId: string
  listenPort: string
  initialRoom: string
  startupPeer: string
  trackerMode: 'default' | 'disabled'
  lanDiscoveryEnabled: boolean
  configPreview: string
  errorNote?: string
  isSaving: boolean
  onNicknameChange: (value: string) => void
  onMeshIdChange: (value: string) => void
  onListenPortChange: (value: string) => void
  onInitialRoomChange: (value: string) => void
  onStartupPeerChange: (value: string) => void
  onTrackerModeChange: (value: 'default' | 'disabled') => void
  onLanDiscoveryChange: (value: boolean) => void
  onSave: () => void
  onSkip: () => void
}

export function OnboardingScreen({
  nickname,
  meshId,
  listenPort,
  initialRoom,
  startupPeer,
  trackerMode,
  lanDiscoveryEnabled,
  configPreview,
  errorNote,
  isSaving,
  onNicknameChange,
  onMeshIdChange,
  onListenPortChange,
  onInitialRoomChange,
  onStartupPeerChange,
  onTrackerModeChange,
  onLanDiscoveryChange,
  onSave,
  onSkip,
}: OnboardingScreenProps) {
  return (
    <main className="shell onboarding-shell">
      <section className="panel onboarding-hero">
        <div>
          <p className="eyebrow">Welcome</p>
          <h1>Moss Chat Dev</h1>
          <p className="runtime-summary">
            Configure your node once and launch straight into a live Moss session.
            This onboarding is the primary entry flow for the desktop app.
          </p>
        </div>
        <div className="hero-meta hero-meta-left">
          <span>Shared runtime</span>
          <span>Direct P2P</span>
          <span>Tracker + LAN bootstrap</span>
        </div>
      </section>

      <RuntimeSetupPanel
        nickname={nickname}
        meshId={meshId}
        listenPort={listenPort}
        initialRoom={initialRoom}
        startupPeer={startupPeer}
        trackerMode={trackerMode}
        lanDiscoveryEnabled={lanDiscoveryEnabled}
        configPreview={configPreview}
        errorNote={errorNote}
        isSaving={isSaving}
        primaryActionLabel={isSaving ? 'Applying and starting...' : 'Apply and start runtime'}
        secondaryActionLabel="Open shell without starting"
        onNicknameChange={onNicknameChange}
        onMeshIdChange={onMeshIdChange}
        onListenPortChange={onListenPortChange}
        onInitialRoomChange={onInitialRoomChange}
        onStartupPeerChange={onStartupPeerChange}
        onTrackerModeChange={onTrackerModeChange}
        onLanDiscoveryChange={onLanDiscoveryChange}
        onSave={onSave}
        onSecondaryAction={onSkip}
      />
    </main>
  )
}
