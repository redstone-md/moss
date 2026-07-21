import * as THREE from "three";
import { Time, mulberry32 } from "../core.js";

const FOG_COLOR = new THREE.Color("#050d08");
const FOG_DENSITY = 0.026;

const FOG_GLSL = /* glsl */ `
  vec3 applyFog(vec3 col, float dist) {
    float f = 1.0 - exp(-dist * dist * ${FOG_DENSITY} * ${FOG_DENSITY});
    return mix(col, vec3(0.0196, 0.051, 0.031), f);
  }
`;

const nodeVert = /* glsl */ `
  attribute float aSeed;
  attribute float aHealth;
  attribute float aFlash;
  uniform float uTime;
  varying vec3 vN;
  varying vec3 vV;
  varying float vSeed;
  varying float vFlash;
  varying float vDist;

  void main() {
    vSeed = aSeed;
    vFlash = aFlash;
    float breathe = 1.0 + 0.07 * sin(uTime * 2.0 + aSeed * 31.4);
    float s = aHealth * breathe;
    vec3 pos = position * s;
    vec4 wp = instanceMatrix * vec4(pos, 1.0);
    vec4 mv = viewMatrix * wp;
    vN = normalize((viewMatrix * instanceMatrix * vec4(normal, 0.0)).xyz);
    vV = normalize(-mv.xyz);
    vDist = -mv.z;
    gl_Position = projectionMatrix * mv;
  }
`;

const nodeFrag = /* glsl */ `
  uniform float uTime;
  varying vec3 vN;
  varying vec3 vV;
  varying float vSeed;
  varying float vFlash;
  varying float vDist;
  ${FOG_GLSL}

  void main() {
    float fres = pow(1.0 - abs(dot(normalize(vN), normalize(vV))), 2.2);
    vec3 core = vec3(0.015, 0.10, 0.05);
    vec3 rim = vec3(0.42, 1.0, 0.48);
    float flicker = 0.75 + 0.25 * sin(uTime * (1.5 + vSeed) + vSeed * 40.0);
    vec3 col = core + rim * fres * flicker;
    col += vec3(0.85, 1.0, 0.8) * vFlash;      // self-heal burst flash
    col = applyFog(col, vDist);
    gl_FragColor = vec4(col, 1.0);
  }
`;

const edgeVert = /* glsl */ `
  attribute float aParam;
  attribute float aSeed;
  attribute float aHealth;
  varying float vParam;
  varying float vSeed;
  varying float vHealth;
  varying float vDist;

  void main() {
    vParam = aParam;
    vSeed = aSeed;
    vHealth = aHealth;
    vec4 mv = modelViewMatrix * vec4(position, 1.0);
    vDist = -mv.z;
    gl_Position = projectionMatrix * mv;
  }
`;

const edgeFrag = /* glsl */ `
  uniform float uTime;
  uniform float uActivity;
  varying float vParam;
  varying float vSeed;
  varying float vHealth;
  varying float vDist;
  ${FOG_GLSL}

  float pulse(float t, float offset, float speed) {
    float p = fract(uTime * speed + offset);
    float d = t - p;
    return exp(-d * d * 380.0);
  }

  void main() {
    vec3 base = vec3(0.10, 0.34, 0.16);
    vec3 packetA = vec3(0.55, 1.0, 0.45);
    vec3 packetB = vec3(0.35, 0.95, 0.75);

    float spd = 0.10 + fract(vSeed * 7.13) * 0.16;
    float p1 = pulse(vParam, vSeed, spd);
    float p2 = pulse(vParam, vSeed + 0.47, spd * 1.7) * step(0.55, fract(vSeed * 3.7));

    vec3 col = base * 0.35;
    col += packetA * p1 * (1.2 + uActivity);
    col += packetB * p2;

    float fade = exp(-vDist * 0.030);           // distance fade for additive lines
    col *= vHealth * fade;
    col = applyFog(col, vDist) * vHealth;
    gl_FragColor = vec4(col, 1.0);
  }
`;

export class Network {
  constructor(scene) {
    this.scene = scene;
    this.rand = mulberry32(1337);
    this.nodes = [];       // {pos: Vector3, scale, health, targetHealth, flash, deadUntil}
    this.edges = [];       // {a, b}
    this.activity = 0;     // bumped on kill, feeds edge shader

    this._generate();
    this._buildNodeMesh();
    this._buildEdgeLines();

    this.raycaster = new THREE.Raycaster();
    this.aliveCount = this.nodes.length;
  }

  _generate() {
    const r = this.rand;
    // organic blob clusters strung along the camera path (z: +6 → -22)
    const clusters = [
      { c: new THREE.Vector3(0, 2.5, 5), rad: 6.0, n: 80 },
      { c: new THREE.Vector3(-2.5, 2.0, -5), rad: 6.5, n: 90 },
      { c: new THREE.Vector3(1.5, 3.0, -16), rad: 6.0, n: 80 },
    ];
    for (const cl of clusters) {
      for (let i = 0; i < cl.n; i++) {
        // rejection-free gaussian-ish blob
        const u = r(), v = r(), w = r();
        const th = u * Math.PI * 2;
        const ph = Math.acos(2 * v - 1);
        const rr = cl.rad * Math.cbrt(w);
        const p = new THREE.Vector3(
          cl.c.x + rr * Math.sin(ph) * Math.cos(th),
          cl.c.y + rr * Math.sin(ph) * Math.sin(th) * 0.62,
          cl.c.z + rr * Math.cos(ph)
        );
        this.nodes.push(this._makeNode(p, r));
      }
    }
    // bridge nodes between clusters
    for (let i = 0; i < 24; i++) {
      const t = r();
      const z = 6 - 24 * t;
      const p = new THREE.Vector3((r() - 0.5) * 7, 1.5 + (r() - 0.5) * 4, z + (r() - 0.5) * 3);
      this.nodes.push(this._makeNode(p, r));
    }

    // edges: k-nearest within radius, degree-capped
    const maxDist = 4.4;
    const degree = new Array(this.nodes.length).fill(0);
    const seen = new Set();
    for (let i = 0; i < this.nodes.length; i++) {
      const dists = [];
      for (let j = 0; j < this.nodes.length; j++) {
        if (i === j) continue;
        const d = this.nodes[i].pos.distanceTo(this.nodes[j].pos);
        if (d < maxDist) dists.push([d, j]);
      }
      dists.sort((a, b) => a[0] - b[0]);
      const want = 2 + Math.floor(r() * 2);
      for (let k = 0; k < Math.min(want, dists.length); k++) {
        const j = dists[k][1];
        if (degree[i] > 4 || degree[j] > 4) continue;
        const key = i < j ? i * 100000 + j : j * 100000 + i;
        if (seen.has(key)) continue;
        seen.add(key);
        this.edges.push({ a: i, b: j });
        degree[i]++;
        degree[j]++;
      }
    }
  }

  _makeNode(pos, r) {
    const supernode = r() > 0.93;
    return {
      pos,
      scale: supernode ? 1.9 + r() * 0.6 : 0.6 + r() * 0.8,
      seed: r(),
      health: 1,
      targetHealth: 1,
      flash: 0,
      deadUntil: 0,
    };
  }

  _buildNodeMesh() {
    const geo = new THREE.IcosahedronGeometry(0.13, 1);
    const n = this.nodes.length;
    this.healthAttr = new THREE.InstancedBufferAttribute(new Float32Array(n), 1);
    this.flashAttr = new THREE.InstancedBufferAttribute(new Float32Array(n), 1);
    const seedAttr = new THREE.InstancedBufferAttribute(new Float32Array(n), 1);

    for (let i = 0; i < n; i++) {
      this.healthAttr.array[i] = 1;
      seedAttr.array[i] = this.nodes[i].seed;
    }
    geo.setAttribute("aHealth", this.healthAttr);
    geo.setAttribute("aFlash", this.flashAttr);
    geo.setAttribute("aSeed", seedAttr);

    this.nodeMat = new THREE.ShaderMaterial({
      vertexShader: nodeVert,
      fragmentShader: nodeFrag,
      uniforms: { uTime: { value: 0 } },
    });

    this.nodeMesh = new THREE.InstancedMesh(geo, this.nodeMat, n);
    const m = new THREE.Matrix4();
    const q = new THREE.Quaternion();
    const s = new THREE.Vector3();
    for (let i = 0; i < n; i++) {
      const nd = this.nodes[i];
      s.setScalar(nd.scale);
      m.compose(nd.pos, q, s);
      this.nodeMesh.setMatrixAt(i, m);
    }
    this.nodeMesh.instanceMatrix.needsUpdate = true;
    this.scene.add(this.nodeMesh);
  }

  _buildEdgeLines() {
    const e = this.edges.length;
    const pos = new Float32Array(e * 2 * 3);
    const param = new Float32Array(e * 2);
    const seed = new Float32Array(e * 2);
    this.edgeHealth = new Float32Array(e * 2).fill(1);

    for (let i = 0; i < e; i++) {
      const { a, b } = this.edges[i];
      const pa = this.nodes[a].pos, pb = this.nodes[b].pos;
      pos.set([pa.x, pa.y, pa.z, pb.x, pb.y, pb.z], i * 6);
      param[i * 2] = 0;
      param[i * 2 + 1] = 1;
      const sd = this.rand();
      seed[i * 2] = sd;
      seed[i * 2 + 1] = sd;
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    geo.setAttribute("aParam", new THREE.BufferAttribute(param, 1));
    geo.setAttribute("aSeed", new THREE.BufferAttribute(seed, 1));
    this.edgeHealthAttr = new THREE.BufferAttribute(this.edgeHealth, 1);
    geo.setAttribute("aHealth", this.edgeHealthAttr);

    this.edgeMat = new THREE.ShaderMaterial({
      vertexShader: edgeVert,
      fragmentShader: edgeFrag,
      uniforms: {
        uTime: { value: 0 },
        uActivity: { value: 0 },
      },
      blending: THREE.AdditiveBlending,
      transparent: true,
      depthWrite: false,
    });

    this.edgeLines = new THREE.LineSegments(geo, this.edgeMat);
    this.scene.add(this.edgeLines);
  }

  // click-to-kill: raycast instanced nodes, returns true on hit
  tryKill(ndc, camera) {
    this.raycaster.setFromCamera(new THREE.Vector2(ndc.x, ndc.y), camera);
    this.raycaster.params.Mesh = { threshold: 0.4 };
    const hits = this.raycaster.intersectObject(this.nodeMesh);
    if (!hits.length) return false;
    const id = hits[0].instanceId;
    const nd = this.nodes[id];
    if (nd.targetHealth === 0) return false;
    nd.targetHealth = 0;
    nd.deadUntil = Time.elapsed + 3.2;
    this.activity = 1.6;
    // neighbors flash — the mesh "notices" and reroutes
    for (const ed of this.edges) {
      if (ed.a === id) this.nodes[ed.b].flash = 1;
      if (ed.b === id) this.nodes[ed.a].flash = 1;
    }
    return true;
  }

  update() {
    const t = Time.elapsed;
    const dt = Time.delta;
    this.nodeMat.uniforms.uTime.value = t;
    this.edgeMat.uniforms.uTime.value = t;

    this.activity = Math.max(0, this.activity - dt * 0.8);
    this.edgeMat.uniforms.uActivity.value = this.activity;

    let dirtyNodes = false;
    let alive = 0;
    for (let i = 0; i < this.nodes.length; i++) {
      const nd = this.nodes[i];
      if (nd.targetHealth === 0 && t > nd.deadUntil) {
        nd.targetHealth = 1; // regrow
        nd.flash = 0.8;
      }
      if (nd.health !== nd.targetHealth) {
        const k = nd.targetHealth > nd.health ? 2.4 : 6.0; // die fast, regrow slower
        nd.health += (nd.targetHealth - nd.health) * Math.min(1, k * dt);
        if (Math.abs(nd.health - nd.targetHealth) < 0.005) nd.health = nd.targetHealth;
        this.healthAttr.array[i] = nd.health;
        dirtyNodes = true;
      }
      if (nd.flash > 0.001) {
        nd.flash *= Math.pow(0.03, dt); // fast decay
        this.flashAttr.array[i] = nd.flash;
        dirtyNodes = true;
      } else if (this.flashAttr.array[i] !== 0) {
        this.flashAttr.array[i] = 0;
        dirtyNodes = true;
      }
      if (nd.health > 0.5) alive++;
    }
    this.aliveCount = alive;

    if (dirtyNodes) {
      this.healthAttr.needsUpdate = true;
      this.flashAttr.needsUpdate = true;
      // edge health = min of endpoints
      for (let i = 0; i < this.edges.length; i++) {
        const h = Math.min(this.nodes[this.edges[i].a].health, this.nodes[this.edges[i].b].health);
        this.edgeHealth[i * 2] = h;
        this.edgeHealth[i * 2 + 1] = h;
      }
      this.edgeHealthAttr.needsUpdate = true;
    }
  }
}

export { FOG_COLOR, FOG_GLSL };
