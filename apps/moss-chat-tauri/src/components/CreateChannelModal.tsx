type CreateChannelModalProps = {
  roomDraft: string
  isCreating: boolean
  errorNote?: string
  onRoomDraftChange: (value: string) => void
  onCreate: () => void
  onClose: () => void
}

export function CreateChannelModal({
  roomDraft,
  isCreating,
  errorNote,
  onRoomDraftChange,
  onCreate,
  onClose,
}: CreateChannelModalProps) {
  return (
    <div
      className="modal-backdrop"
      role="presentation"
      onClick={onClose}
      onKeyDown={(event) => {
        if (event.key === 'Escape' && !isCreating) {
          onClose()
        }
      }}
    >
      <form
        className="panel modal-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-channel-title"
        onClick={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          event.preventDefault()
          onCreate()
        }}
      >
        <div className="panel-header">
          <div>
            <p className="eyebrow">Create channel</p>
            <h2 id="create-channel-title">Open a new room</h2>
          </div>
        </div>
        <p className="runtime-summary">
          Use a concise room name. The channel will be created immediately and the
          conversation will open as soon as you join it.
        </p>
        <label className="field-grid">
          <span>Channel name</span>
          <input
            autoFocus
            value={roomDraft}
            onChange={(event) => onRoomDraftChange(event.target.value)}
            placeholder="design-reviews"
          />
        </label>
        {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
        <div className="setup-actions">
          <button className="primary-action" type="submit" disabled={isCreating}>
            {isCreating ? 'Creating...' : 'Create channel'}
          </button>
          <button className="secondary-action" type="button" onClick={onClose} disabled={isCreating}>
            Cancel
          </button>
        </div>
      </form>
    </div>
  )
}
