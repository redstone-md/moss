type RuntimeSetupPanelProps = {
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
  primaryActionLabel?: string
  secondaryActionLabel?: string
  onNicknameChange: (value: string) => void
  onMeshIdChange: (value: string) => void
  onListenPortChange: (value: string) => void
  onInitialRoomChange: (value: string) => void
  onStartupPeerChange: (value: string) => void
  onTrackerModeChange: (value: 'default' | 'disabled') => void
  onLanDiscoveryChange: (value: boolean) => void
  onSave: () => void
  onSecondaryAction?: () => void
}

export function RuntimeSetupPanel({
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
  primaryActionLabel,
  secondaryActionLabel,
  onNicknameChange,
  onMeshIdChange,
  onListenPortChange,
  onInitialRoomChange,
  onStartupPeerChange,
  onTrackerModeChange,
  onLanDiscoveryChange,
  onSave,
  onSecondaryAction,
}: RuntimeSetupPanelProps) {
  return (
    <section className="panel setup-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Runtime setup</p>
          <h2>Chat bootstrap</h2>
        </div>
      </div>
      <div className="setup-grid">
        <label className="field-grid">
          <span>Nickname</span>
          <input
            value={nickname}
            onChange={(event) => onNicknameChange(event.target.value)}
            placeholder="operator"
          />
        </label>
        <label className="field-grid">
          <span>Mesh ID</span>
          <input
            value={meshId}
            onChange={(event) => onMeshIdChange(event.target.value)}
            placeholder="moss-chat-dev"
          />
        </label>
        <label className="field-grid">
          <span>Listen port</span>
          <input
            value={listenPort}
            onChange={(event) => onListenPortChange(event.target.value)}
            placeholder="0"
            inputMode="numeric"
          />
        </label>
        <label className="field-grid">
          <span>Initial room</span>
          <input
            value={initialRoom}
            onChange={(event) => onInitialRoomChange(event.target.value)}
            placeholder="lobby"
          />
        </label>
        <label className="field-grid">
          <span>Startup peer</span>
          <input
            value={startupPeer}
            onChange={(event) => onStartupPeerChange(event.target.value)}
            placeholder="host:port"
          />
        </label>
        <label className="field-grid">
          <span>Tracker bootstrap</span>
          <select
            value={trackerMode}
            onChange={(event) =>
              onTrackerModeChange(event.target.value as 'default' | 'disabled')
            }
          >
            <option value="default">Use built-in trackers</option>
            <option value="disabled">Disable trackers</option>
          </select>
        </label>
        <label className="toggle-field">
          <input
            type="checkbox"
            checked={lanDiscoveryEnabled}
            onChange={(event) => onLanDiscoveryChange(event.target.checked)}
          />
          <span>Allow LAN discovery beacons</span>
        </label>
      </div>
      <div className="preview-card">
        <p className="eyebrow">Config preview</p>
        <pre>{configPreview}</pre>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
      <div className="setup-actions">
        <button className="primary-action" onClick={onSave} disabled={isSaving}>
          {primaryActionLabel ?? (isSaving ? 'Saving...' : 'Apply settings')}
        </button>
        {onSecondaryAction ? (
          <button
            className="secondary-action"
            onClick={onSecondaryAction}
            disabled={isSaving}
            type="button"
          >
            {secondaryActionLabel ?? 'Cancel'}
          </button>
        ) : null}
      </div>
    </section>
  )
}
