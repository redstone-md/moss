import type { RuntimeDiagnostics } from '../lib/schemas'

type DiagnosticsPanelProps = {
  diagnostics: RuntimeDiagnostics
}

export function DiagnosticsPanel({ diagnostics }: DiagnosticsPanelProps) {
  return (
    <section className="panel diagnostics-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Diagnostics</p>
          <h2>Runtime view</h2>
        </div>
      </div>
      <div className="diagnostics-grid">
        <div className="diagnostic-card">
          <span>Configured mesh</span>
          <strong>{diagnostics.configuredMeshId}</strong>
          <p>
            room {diagnostics.initialRoom} | port {diagnostics.configuredListenPort}
          </p>
        </div>
        <div className="diagnostic-card">
          <span>Bootstrap</span>
          <strong>{diagnostics.trackerMode}</strong>
          <p>LAN discovery {diagnostics.lanDiscovery}</p>
        </div>
        <div className="diagnostic-card">
          <span>Active runtime</span>
          <strong>{diagnostics.activeMeshId}</strong>
          <p>listen {diagnostics.activeListenPort}</p>
        </div>
        <div className="diagnostic-card">
          <span>Peer state</span>
          <strong>
            {diagnostics.peerCount} peers / {diagnostics.channelCount} channels
          </strong>
          <p>
            {diagnostics.supernodeReady
              ? 'Relay candidate ready'
              : 'Relay candidate offline'}
          </p>
        </div>
      </div>
      <div className="hero-meta hero-meta-left">
        <span>
          startup peer{' '}
          {diagnostics.startupPeer === 'not set'
            ? 'not configured'
            : diagnostics.startupPeer}
        </span>
        {diagnostics.activeChannels.length > 0 ? (
          diagnostics.activeChannels.map((channel) => (
            <span key={channel}>#{channel}</span>
          ))
        ) : (
          <span>No active subscriptions yet</span>
        )}
      </div>
    </section>
  )
}
