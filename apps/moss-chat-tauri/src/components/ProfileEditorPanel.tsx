type ProfileEditorPanelProps = {
  nickname: string
  avatarPreviewUrl: string | null
  avatarFileName: string | null
  meshId: string
  initialRoom: string
  startupPeer: string
  listenPort: string
  trackerMode: 'default' | 'disabled'
  lanDiscoveryEnabled: boolean
  configPreview: string
  errorNote?: string
  isSaving: boolean
  onAvatarChange: (file: File | null) => void
  onNicknameChange: (value: string) => void
  onMeshIdChange: (value: string) => void
  onInitialRoomChange: (value: string) => void
  onStartupPeerChange: (value: string) => void
  onListenPortChange: (value: string) => void
  onTrackerModeChange: (value: 'default' | 'disabled') => void
  onLanDiscoveryChange: (value: boolean) => void
  onSave: () => void
}

export function ProfileEditorPanel({
  nickname,
  avatarPreviewUrl,
  avatarFileName,
  meshId,
  initialRoom,
  startupPeer,
  listenPort,
  trackerMode,
  lanDiscoveryEnabled,
  configPreview,
  errorNote,
  isSaving,
  onAvatarChange,
  onNicknameChange,
  onMeshIdChange,
  onInitialRoomChange,
  onStartupPeerChange,
  onListenPortChange,
  onTrackerModeChange,
  onLanDiscoveryChange,
  onSave,
}: ProfileEditorPanelProps) {
  return (
    <section className="panel profile-editor">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Edit profile</p>
          <h2>Identity and bootstrap</h2>
        </div>
      </div>

      <div className="profile-preview">
        <div className="profile-avatar-preview-shell">
          {avatarPreviewUrl ? (
            <img
              className="profile-avatar-image"
              src={avatarPreviewUrl}
              alt="Profile preview"
            />
          ) : (
            <div className="profile-avatar-preview">{avatarLabel(nickname)}</div>
          )}
        </div>
        <div>
          <strong>{nickname || 'operator'}</strong>
          <p className="runtime-summary">
            Live preview of how your identity appears in the channel list and message
            stream.
          </p>
          <p className="runtime-summary">
            {avatarFileName ? `Photo ready: ${avatarFileName}` : 'No profile photo selected yet.'}
          </p>
        </div>
      </div>

      <div className="setup-grid">
        <label className="field-grid">
          <span>Profile photo</span>
          <input
            type="file"
            accept="image/*"
            onChange={(event) => onAvatarChange(event.target.files?.[0] ?? null)}
          />
        </label>
        <label className="field-grid">
          <span>Display name</span>
          <input value={nickname} onChange={(event) => onNicknameChange(event.target.value)} />
        </label>
        <label className="field-grid">
          <span>Mesh ID</span>
          <input value={meshId} onChange={(event) => onMeshIdChange(event.target.value)} />
        </label>
        <label className="field-grid">
          <span>Initial room</span>
          <input
            value={initialRoom}
            onChange={(event) => onInitialRoomChange(event.target.value)}
          />
        </label>
        <label className="field-grid">
          <span>Startup peer</span>
          <input
            value={startupPeer}
            onChange={(event) => onStartupPeerChange(event.target.value)}
            placeholder="optional host:port"
          />
        </label>
        <label className="field-grid">
          <span>Listen port</span>
          <input
            value={listenPort}
            onChange={(event) => onListenPortChange(event.target.value)}
            inputMode="numeric"
          />
        </label>
        <label className="field-grid">
          <span>Bootstrap mode</span>
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
          <span>Enable LAN discovery for nearby peers</span>
        </label>
      </div>

      <div className="preview-card">
        <p className="eyebrow">Config preview</p>
        <pre>{configPreview}</pre>
      </div>

      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}

      <div className="setup-actions">
        <button className="primary-action" type="button" onClick={onSave} disabled={isSaving}>
          {isSaving ? 'Saving...' : 'Save profile'}
        </button>
      </div>
    </section>
  )
}

function avatarLabel(nickname: string): string {
  const trimmed = nickname.trim()
  if (!trimmed) {
    return 'MC'
  }
  return trimmed
    .split(/[\s._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((chunk) => chunk[0]?.toUpperCase() ?? '')
    .join('')
}
