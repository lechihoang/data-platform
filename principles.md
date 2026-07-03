# Principles for beautiful draw.io diagrams (AWS & system architecture)

Goal: the AI produces draw.io XML with **correct stencil names**, **clean layout**, and a **readable flow** on the first try.

## 0. Mandatory workflow for the AI

0. **Match a template first.** If the request fits an archetype with a template (`diagram-types.md` → "Templates"), open that `examples/<domain>/*.mjs` (grouped `aws/`·`azure/`·`gcp/`·`multicloud/`·`bpmn/`; see `examples/README.md`), reproduce its structure, and run the **Reproduction loop** (match → build → validate → conform-to-checklist → fix, repeat). Don't free-hand a pattern a template already encodes.
1. **Look up every icon via `search_icon`** — do NOT recall or invent stencil names. Paste the exact `style` string returned.
2. Build the XML following the grid and rules below.
3. **Call `validate_diagram`** before returning the result. If there are stencil `errors`/`warnings`, fix and re-validate.

## 1. Grid, alignment & sizing

- Prioritize **relative alignment over absolute grid**: nodes in the same row share one `y`, nodes in the same column share one `x`. (Exact multiples of 10 matter less than things lining up with each other.)
- **Sibling blocks placed side by side must be the SAME HEIGHT** — container frames in a `row` share a common top *and* bottom edge (a shorter block is padded, not left short). The layout engine enforces this automatically (row-sibling `group`/`frame`/`grid`/`pool` children stretch to the tallest); if you hand-place, match their heights yourself. (Leaf icons/boxes keep their natural size, top-aligned.)
- Standard icon size: pick **one** size and reuse it — **78×78** for primary services, 48×48 for compact. Do NOT mix many icon widths in one diagram.
- Minimum spacing between icons: **80px horizontal**, **90px vertical** (leave room for the label under the icon).
- Keep node sizes consistent; avoid one oversized box dominating. **Do not stretch a giant full-width banner** — size elements to their content.
- Resource icons **must** include `aspect=fixed` so they don't distort on resize.

## 2. Flow direction

- Default **left → right** for data pipelines / request flows; **top → bottom** for tiered layering.
- Keep one consistent direction; avoid back-pointing arrows unless they represent feedback/sync (use dashed lines).

## 3. Group with official containers

- Use real AWS **group shapes** (`search_icon --kind group`): `group_aws_cloud_alt`, `group_region`, `group_vpc`, `group_availability_zone`, `group_security_group`, `group_public_subnet`/`group_private_subnet`...
- Nest in the real order: **AWS Cloud → Region → VPC → AZ → Subnet → Security Group**.
- Group frames use `verticalAlign=top;align=left;spacingLeft=30` so the label sits next to the corner icon.
- Declare containers **first** (lower z-index) so they sit beneath their child icons.

## 4. Color — restrained & theme-aware

- Icons keep their **category** color (Compute orange, Storage green, Database pink, Security red, Networking purple, Management magenta...). `search_icon` returns the correct one. Don't recolor icons arbitrarily.
- For **backgrounds/frames/notes**, use a **small cohesive palette** — a few neutral greys plus one or two soft accents. Do NOT scatter many ad-hoc pastel fills (palette sprawl reads as noise). Target ≤ ~8 distinct fill colors per diagram.
- **Pipeline/stage layers MAY carry a soft tint per stage** — the classic pale progression (light green → amber → yellow → purple) reads as ordered stages and looks good *when the tints are pale and cohesive*. That is desirable, not "rainbow". What to avoid is the **garish** look: saturated/clashing fills, a different colour on every small box, or colour with no meaning. For non-stage containers (Region/VPC/account), neutral grey or the AWS group stencil's own light fill is safest — let the service icons carry most of the colour.
- Prefer theme-aware tokens like `fillColor=light-dark(#fbe7d4, #3a2a16)` for backgrounds/accents so the diagram looks right in **both light and dark mode**.
- Reserve strong color for emphasis/notes (e.g. a red `#f8cecc` note box), not for every box. Use `fillOpacity` 20–40 on frames.

## 5. Labels & typography

- Service labels go **below the icon** (`verticalLabelPosition=bottom;verticalAlign=top`), kept short: service name + (role).
- **Limit to 3–4 font sizes** and keep label text **≤ 14px**; never jump to oversized (18+) titles inside the canvas — put a title in its own area.
- Long notes/constraints go in a separate **note box**, never crammed into the icon label.
- Third-party components (no AWS icon) → rounded box, clearly noting "(on EKS)"/"(on EC2)".

## 6. Edges — corner style and routing are *intentional*

- Base style: `edgeStyle=orthogonalEdgeStyle;html=1`.
- **Choose the corner by role, don't blanket-round everything:**
  - Sequential / pipeline flow → `rounded=1` (soft corners).
  - **Fan-out / bus / tree branches (one source → many targets) → `rounded=0`** (sharp right angles). This is the single biggest "looks hand-made vs auto" tell.
- **Pin connection points** (`exitX/exitY` + `entryX/entryY`) for parallel, fan-out, or bus edges so the lines leave/enter at aligned anchors instead of floating and wandering. (e.g. exit bottom-center = `exitX=0.5;exitY=1`.)
- Auto-route simple flows, but in **dense / error-handling diagrams add deliberate waypoints** to avoid line crossings and overlaps — don't rely purely on auto-route there.
- **Labels on bent (L/Z) edges:** a label defaults to the arc midpoint, which on a bent edge lands on the corner or against a box — looks off-center. Add **one waypoint at the centre of the corridor** between the two columns/rows so the perpendicular run is centred and the label sits cleanly on it (always with `labelBackgroundColor`). `validate_diagram` flags labelled bent edges that have no waypoint.
- **Solid** = primary data/control flow; **dashed** = sync/dependency/policy enforcement/lineage. Color edges by source layer to trace them.
- Double-headed arrows (`startArrow=block;endArrow=block`) for bidirectional links (Direct Connect, metadata sync).

## 7. Managed vs self-managed

- **AWS managed** services: use the AWS icon + (optional) a "▸ managed" label.
- OSS software running on EKS/EC2: use a text box, and optionally place it next to the EKS/EC2 icon to show where it runs.

## 8. Recommended overall layout

- Left: **sources/clients**. Center: the **AWS Cloud frame** holding the pipeline plus cross-cutting layers. Right: **consumer systems**.
- Cross-cutting layers (security, monitoring, governance, CI/CD) sit as their own band/column, connected with dashed lines to the relevant components.
- Hybrid/DR: place the other site as a separate block, connected through a Direct Connect node.

## 9. Self-check (before returning)

Run `validate_diagram` and clear both `errors`/`warnings` and the `audit.advice` list:

- [ ] Every `resIcon`/`grIcon` came from `search_icon` (validate reports no errors).
- [ ] No icon is missing `aspect=fixed`; one consistent icon size.
- [ ] All cell `id` values are unique — no two `mxCell` elements share the same `id` (validate now catches this as an error).
- [ ] No edge points to a non-existent id.
- [ ] Icon colors match their category; backgrounds use ≤ ~8 cohesive colors (consider `light-dark()`).
- [ ] ≤ 4 font sizes, no oversized (≥16) label text.
- [ ] Fan-out/bus edges use sharp corners (`rounded=0`) + pinned connection points.
- [ ] One consistent flow direction, edge labels are meaningful.


---

# AWS architecture diagram preset

Conventions specific to AWS architecture diagrams. Layer these on top of the general `principles.md`.

## Containers — nest in the real order

Use the official AWS group shapes (`search_icon "<name>" --kind group`) and **nest them by parent-child**, not by stacking:

```text
AWS Cloud (group_aws_cloud_alt)
└─ Region (group_region, dashed)
   └─ VPC (group_vpc)
      └─ Availability Zone (group_availability_zone, dashed)
         └─ Subnet (group_subnet — color auto-set by label: "Public"→blue, "Private"→green; NEVER pass fill manually)
            └─ Security Group (group_security_group, dashed)
               └─ service icons
```

- A child sets `parent="<containerId>"` and uses coordinates **relative to its container**.
- Don't put a Subnet directly under AWS Cloud, or a Security Group outside a Subnet — the validator flags broken nesting.
- Managed/global services (S3, IAM, KMS, CloudWatch, Route 53, Organizations) live **outside the VPC** — place them in the AWS Cloud band, not inside a subnet.

## Icon color = identity — never recolor

Each AWS icon ships with its official category color (Compute orange, Storage green, Database pink, Security red, Networking purple, Management magenta...). The catalog style already carries the correct `fillColor`. **Do not override it** — a recolored S3 icon is a recognizability bug, and the validator flags it.

Category colors: Compute/Containers `#ED7100` · Storage `#7AA116` · Database `#C925D1` · Networking & Analytics `#8C4FFF` · Security `#DD344C` · Management & App-Integration `#E7157B` · Migration/ML `#01A88D`.

## Canonical layouts

- **Data pipeline (left → right):** Sources → Ingestion → Processing → Storage → Integration/Serving → Consumers. Cross-cutting layers (Security, Monitoring, Governance, CI/CD) as a band below, dashed links to the components they touch.
- **VPC / network diagram:** Each **Availability Zone is a vertical COLUMN**, the AZs sit **side by side**, and the **VPC is the horizontal box** wrapping them (Region → VPC → AZ columns → subnets). Inside an AZ, subnets are **tiers stacked top→bottom** (Public → App → Data); keep the **same tier aligned horizontally across AZs** (public-a level with public-b). Users/Internet sit outside the VPC; a shared ALB/NAT/bus spans **horizontally across the AZ columns**.
- **Event-driven / bus:** put the bus (Kafka/MSK/EventBridge/SNS) in the **center** of the producer/consumer row; producers connect from one side (`exitX=1`), consumers from the other (`exitX=0`) — no crossings.
- **Hybrid / DR:** on-prem / external sites are a SEPARATE block placed OUTSIDE the AWS Region/Cloud container — never nest on-prem inside the Region. Put a Direct Connect / Site-to-Site VPN **node** between cloud and on-prem as the connection channel (not just a labelled edge). See `examples/aws/build_hybrid.mjs`.

## Multi-AZ

- For HA, draw **≥2 Availability Zone columns side by side** inside the VPC and mirror the stateful tier in each (same tier on the same row across AZs). Label AZ-a / AZ-b.
- Stateless services scale horizontally inside each AZ; managed data services (RDS Multi-AZ, etc.) span AZs — show one icon at the VPC level with a note, or one per AZ with a sync link.

## Edges in AWS diagrams

- Pipeline flow → `rounded=1`. Fan-out to multiple targets / bus → `rounded=0` + pinned `exitX/entryX` (see `principles.md` §6).
- Data-flow diagrams read well with `flowAnimation=1` on the main pipeline edges (animates in SVG / desktop).
- Solid = data/control flow; dashed = policy/lineage/sync/DR.
- **Connect to the bounding box, not each replica.** When a multi-AZ stack is wrapped in a dashed `clusterBox` (the per-app / node-group / cluster frame that spans the AZs), point edges at the BOX's id — **one tidy arrow to the border** — instead of drawing a separate arrow to the same component's icon in every AZ. The frame already says "this is N replicas across the AZs", so a single edge to it reads cleanly; N arrows to N child icons just clutter. Create the `clusterBox`es **before** `d.link(...)` so the box ids exist as edge targets. (A genuine fan-out to *distinct* services still combs as usual — this rule is only about the per-AZ replicas of one stack.)

## Placement — keep edges short (avoid the "long detour" smell)

The layout engine places by declared nesting; it does **not** move nodes to shorten edges. So *you* must place connected things near each other:

- **Shared resources** (ECR, S3, CloudWatch, registries, KMS) used by many components: put them in a **band immediately next to their consumers** (e.g. right under the compute area), **not** in a far-away row at the bottom — otherwise every reference becomes a long detour line.
- Put a node **next to what it talks to most**; order layers/columns along the real flow so the spine is short and straight.
- Group repeated cross-cutting links (a node → many, or many → a node) so they comb instead of fanning across the whole canvas.

`validate_diagram` flags this automatically: **"Long connector(s) spanning most of the diagram"** (a node parked too far) and **"N edge crossings"** (tangled flow). Both mean *reposition nodes*, not *reroute edges* — fix placement and re-validate.


---

# Diagram types — layout & routing presets

Different diagram types need different layout and **edge routing**, not one strategy for all. Pick the type first, then apply its preset (`src/types.mjs` → `typePreset(name)`); the routing helpers in `src/layout.mjs` do the geometry.

## Pick the type

| User intent | Type key |
|---|---|
| Data/request pipeline, ETL, request-response across tiers | `pipeline` |
| Org structure, Landing Zone, account/OU hierarchy | `hierarchy` |
| VPC / network topology, Multi-AZ deployment, 3-tier in a VPC | `network` |
| Event-driven, message bus, fan-in/fan-out around a hub | `hubspoke` |
| Hybrid / disaster recovery (on-prem ↔ cloud, two sites) | `hybrid` |
| Multi-account connectivity / service mesh (VPC Lattice, TGW, peering, RAM share) | `mesh` |
| Numbered request walkthrough over an architecture | `sequence` |

## Templates — copy-paste starting points (`examples/`)

Before free-handing, check if a template matches the request. Open it, **reproduce its structure**, then adapt the labels / LAYERS block. Faster, and keeps the house style consistent.

| You're drawing | Start from |
|---|---|
| A **Multi-AZ workload layer** — AZ private-subnet columns · pods on EC2 worker nodes · per-app cross-AZ `clusterBox` · GitOps band | `examples/aws/build_multiaz_template.mjs` |
| A **multi-account Landing Zone / hub-and-spoke** — Network account + **Transit Gateway** · Ingress/Inspection/Egress VPCs · workload spokes · hybrid (DX/VPN) · governance — incl. a **multi-tab SA deck** (As-Is · To-Be · Networking · Security · Backup · Logging · CI/CD) | `examples/aws/build_landingzone_hubspoke_template.mjs` |
| A single VPC (Multi-AZ · EKS · NAT) | `examples/aws/build_vpc_eks.mjs` |
| Hybrid / DR (on-prem ↔ cloud, two sites) | `examples/aws/build_hybrid.mjs` |
| Multi-account mesh / TGW connectivity | `examples/aws/build_mesh.mjs` |

## Reproduction loop — build → validate → conform → fix (repeat)

When a template matches, **don't free-hand it — reproduce it and self-check** in a loop:

1. **Match** — pick the template above for the diagram type; open it.
2. **Build** — reproduce its structure with the layout engine + helpers (`clusterBox`, themed creators); never hardcode coordinates.
3. **Validate** — `validate_diagram` (or `d.validate()`); clear all `errors`, `warnings`, `audit.advice`.
4. **Conform** — `render_diagram`, then check the output against the archetype checklist below.
5. **Fix & repeat** — until validate is clean **and** every checklist item passes (≤ ~3 rounds).

### Conformance checklist

#### Multi-AZ workload layer

- [ ] AZ are vertical columns; each AZ = a private subnet (bank/internal style = NO public subnet/NAT).
- [ ] 1 EC2 worker node per AZ holds the app pods (real icons), mirrored across AZs.
- [ ] Each app pod has its OWN dashed cross-AZ `clusterBox` (colour-coded); an outer EKS node-group box spans the worker nodes; non-EKS stacks (Kafka…) get their own box.
- [ ] EDGES connect to the dashed BOX border (`comp_<app>` / `eksstack` / stack id) — ONE arrow — never to each per-AZ icon.
- [ ] Managed AWS services sit OUTSIDE the VPC; optional GitOps band (Terraform + ArgoCD).

#### Multi-account Landing Zone (hub-and-spoke)

- [ ] Accounts separated via `group_account`: Network (hub) + Workload (spokes) + Security / shared-services.
- [ ] Transit Gateway is the hub; Ingress (WAF/ALB) · Inspection (NGFW) · Egress (NAT) VPCs live in the Network account.
- [ ] Workload VPCs attach to the TGW (spokes); on-prem reaches the TGW via Direct Connect + Site-to-Site VPN.
- [ ] Governance baseline present: CloudTrail · Config · GuardDuty · Security Hub · KMS.
- [ ] Edges go to the Transit Gateway (hub-and-spoke), not node-to-node spaghetti.

## Composing archetypes (real systems mix several)

A real architecture is usually NOT one pure type — it COMBINES them, and the engine composes freely because every archetype is just a nested `group`/`frame` subtree. Build the dominant type, then nest the others inside/around it. `new Diagram(type)` only sets edge-routing defaults (pick the dominant one) — it does **not** restrict the layout.

Example — a full data platform = **pipeline** (layered stages) **inside** an AWS Cloud frame, with a **hybrid** on-prem block + Direct Connect channel beside it, a **mesh** of accounts, and a cross-cutting **band**:

```js
const tree = frame("root", "", { dir: "row", align: "center" }, [
  onpremFrame("op", "On-Premise", [...]),        // hybrid block (outside the Region)
  frame("chn", "", { dir: "col" }, [icon("dx","direct_connect","Direct Connect")]),  // channel
  group("aws", "group_aws_cloud_alt", "AWS Cloud", { dir: "col" }, [
    frame("pipe", "", { dir: "row" }, [stage("s1",0,..), stage("s2",1,..), ...]),    // pipeline
    band("ops", "Security · Ops", [...]),                                            // cross-cutting band
  ]),
]);
```

Don't force a complex system into one archetype — **compose**. Reuse the themed creators (`stage`/`band`/`endpoint`/`onpremFrame`) across the pieces so the whole thing stays one coherent style.

## Frames are square — AWS convention

AWS architecture diagrams use **square-corner** containers, not rounded frames. Use the official group stencils (`group_*`, already square) for Region/Account/VPC/AZ/Subnet, and keep resource boxes square too. The `Diagram` builder's `box()` defaults to square (pass `round:true` only when you deliberately want a rounded shape).

## Per-type layout & routing

### pipeline
- **Layout:** left → right, one column per tier (Ingest → Process → Store → Serve). Cross-cutting layers as a band below.
- **Routing:** put the connected "spine" nodes at the **same Y** so the main flow is straight horizontal. Bent edges use **two waypoints in the inter-column corridor** (`routeLR` with `laneX`). Fan-out from one source → **sharp** corners (`rounded=0`).

### hierarchy
- **Layout:** top → down. Parent (Management/Root) on top; children (OUs/accounts) nested below. Group by OU containers.
- **Routing:** **sharp** corners. All siblings of one parent exit the parent at its bottom-centre and share **one horizontal lane** just below the parent (a bus) before dropping to each child — `routeTB` with a shared `laneY`.

### network
- **Layout:** nest **Region → VPC → Availability Zone → Subnet → Security Group** with real parent-child containment. **Mirror the AZs** (stack them vertically). Tiers flow left → right inside each AZ: Public (NAT/IGW) → Private app → Private data.
- **Routing:** make the **load balancer a tall node spanning the AZs** so its edges to each AZ's app tier are straight horizontals. Edges between tiers are horizontal; go **vertical only to cross AZs** (e.g. RDS primary → standby). Cross-AZ replication / DR uses **dashed** lines. Regional/edge services (WAF, CloudWatch, S3) sit **outside the VPC** but inside the Region.

### hubspoke
- **Layout:** hub (bus/TGW/EventBridge/SNS) in the **centre** of the producer/consumer row.
- **Routing:** producers connect from one side (`exitX=1`), consumers from the other (`exitX=0`) with short horizontal edges → no crossings.

### hybrid
- **Layout:** two site blocks (on-prem, cloud) as separate frames.
- **Routing:** connect through a single **Direct Connect / VPN** node; mirror matching components on both sides; bidirectional links are **dashed double-headed**.

## Fan-out / fan-in edges (the #1 source of ugly diagrams)

The kit routes both automatically — you never compute lanes, just call `d.link(...)`
repeatedly and the builder groups edges by shared endpoint:

- **Fan-out** (1 source → ≥2 same-direction targets): routed as a **comb** — one
  shared trunk lane, one short branch per target, all exiting the source center so
  the collinear segments merge into a single clean trunk.
- **Fan-in** (≥2 same-direction sources → 1 target): the **reverse comb** — edges
  share one lane just before the target and arrive at **distinct entry points**
  (spread `entryY`/`entryX`), so the arrowheads don't stack on one spot.

Both work on either axis (LR: hub→consumers; TB: management account→OUs, org-chart
style). Fan-out wins if an edge qualifies as both. Keep the many-side roughly
**aligned** (same x for LR, same y for TB) so branches stay short — the layout
engine's `frame`/`group`/`grid` already does this.

## Grid layout

When a row of N items doesn't match the column count of a sibling row (e.g. 4 storage
icons under 3 AZ columns), use `grid(id, gname, label, { cols }, children)` instead of
hand-stacking. It lays children into evenly-sized cells (centered), so the frame hugs
the grid tightly with no lop-sided whitespace.

## Validation hooks

`validate_diagram` enforces several of these regardless of type: unknown stencils, recolored icons, broken Region→…→SG nesting, off-centre labels on bent edges, and fan-out edges that should be sharp + pinned. Clear all `audit.advice` before delivering.


---

