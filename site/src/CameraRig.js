import * as THREE from "three";
import { Time, Input } from "./core.js";

// Camera dolly along a Catmull-Rom path through the mesh, look targets on their own
// curve, mouse parallax layered on top (house style: current += k*(target-current)).

const POSITIONS = [
  new THREE.Vector3(0, 2.5, 27),     // 0 hero — the whole organism
  new THREE.Vector3(9, 4.0, 14),     // 1 discover — swing around the flank
  new THREE.Vector3(2.5, 1.2, 3),    // 2 self-healing — inside the mesh
  new THREE.Vector3(-6, 2.5, -4),    // 3 encrypted — deep between clusters
  new THREE.Vector3(-2, 7.0, -12),   // 4 telemetry — rise above, observe
  new THREE.Vector3(4.5, 0.5, -19),  // 5 browser — dive into the far cluster
  new THREE.Vector3(0, -2.8, -28),   // 6 join — through and out, looking back up
];

const TARGETS = [
  new THREE.Vector3(0, 2.2, 0),
  new THREE.Vector3(-1, 2.0, -3),
  new THREE.Vector3(-3, 2.5, -9),
  new THREE.Vector3(0, 2.5, -12),
  new THREE.Vector3(1, 1.5, -16),
  new THREE.Vector3(1, 3.0, -16),
  new THREE.Vector3(0, 2.5, -10),
];

export class CameraRig {
  constructor() {
    this.camera = new THREE.PerspectiveCamera(52, 1, 0.1, 200);
    this.posCurve = new THREE.CatmullRomCurve3(POSITIONS, false, "catmullrom", 0.35);
    this.lookCurve = new THREE.CatmullRomCurve3(TARGETS, false, "catmullrom", 0.35);

    this.parallax = { x: 0, y: 0 };
    this._pos = new THREE.Vector3();
    this._look = new THREE.Vector3();
    this._right = new THREE.Vector3();
    this._up = new THREE.Vector3(0, 1, 0);
  }

  resize(w, h) {
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
  }

  update(progress) {
    const t = Math.min(Math.max(progress, 0), 1);
    this.posCurve.getPoint(t, this._pos);
    this.lookCurve.getPoint(t, this._look);

    // parallax targets from smoothed pointer
    const k = 1 - Math.pow(0.002, Time.delta);
    this.parallax.x += (Input.smooth.x * 1.4 - this.parallax.x) * k;
    this.parallax.y += (Input.smooth.y * 0.9 - this.parallax.y) * k;

    this.camera.position.copy(this._pos);
    this.camera.lookAt(this._look);

    // offset along camera right/up so the frame breathes with the cursor
    this._right.setFromMatrixColumn(this.camera.matrix, 0);
    this.camera.position.addScaledVector(this._right, this.parallax.x);
    this.camera.position.addScaledVector(this._up, this.parallax.y * 0.6);
    this.camera.lookAt(this._look);
  }
}
