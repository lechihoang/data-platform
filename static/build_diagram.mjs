import { writeFileSync } from "fs";
import { Diagram } from "/Users/abc/.agents/skills/drawio-cloud-architect/src/builder.mjs";
import { group, frame, grid, icon, box, renderTree, endpoint, stage } from "/Users/abc/.agents/skills/drawio-cloud-architect/src/layout-engine.mjs";

const d = new Diagram("pipeline");

const tree = frame("root", "", { dir: "row", align: "center", spacingX: 80, spacingY: 80, padding: 40 }, [
  frame("src_col", "", { dir: "col" }, [
    endpoint("nyc_data", "NYC Taxi Data<br>(Parquet/CSV)", { style: "shape=document;fillColor=#fff2cc;strokeColor=#d6b656;html=1;whiteSpace=wrap;", w: 160, h: 80 })
  ]),
  frame("ingest", "", { dir: "col", spacingY: 40 }, [
    stage("stage_nifi", 0, "Ingestion", [
      icon("nifi", "nifi", "Apache NiFi")
    ])
  ]),
  frame("store", "", { dir: "col", spacingY: 40 }, [
    stage("stage_minio", 1, "Storage", [
      icon("minio", "minio", "MinIO<br>(Raw Data)")
    ])
  ]),
  frame("catalog", "", { dir: "col", spacingY: 40 }, [
    stage("stage_nessie", 2, "Data Catalog", [
      icon("nessie", "nessie", "Project Nessie<br>(branch dev)")
    ])
  ]),
  frame("process", "", { dir: "col", spacingY: 40 }, [
    stage("stage_spark", 3, "Transform", [
      frame("spark_iceberg", "", { dir: "col", gap: 20 }, [
        frame("box_spark", "", { dir: "row", pad: 8, gap: 10, fill: "#ffffff", stroke: "#666666" }, [
          box("lbl_spark", "Compute engine", { style: "text;html=1;strokeColor=none;fillColor=none;align=center;verticalAlign=middle;fontSize=11;fontStyle=1;", w: 90, h: 40 }),
          icon("spark", "spark", "")
        ]),
        frame("box_iceberg", "", { dir: "row", pad: 8, gap: 10, fill: "#ffffff", stroke: "#666666" }, [
          box("lbl_iceberg", "Table format", { style: "text;html=1;strokeColor=none;fillColor=none;align=center;verticalAlign=middle;fontSize=11;fontStyle=1;", w: 90, h: 40 }),
          icon("iceberg", "iceberg", "")
        ])
      ])
    ])
  ]),
  frame("dq", "", { dir: "col", spacingY: 40 }, [
    stage("stage_dq", 4, "Data Quality", [
      icon("gx", "greatexpectations", "Great Expectations")
    ])
  ]),
  frame("result", "", { dir: "col", gap: 80 }, [
    box("merge_main", "Merge dev -> main", { style: "shape=ellipse;fillColor=#d5e8d4;strokeColor=#82b366;html=1;whiteSpace=wrap;", w: 140, h: 60 }),
    box("stop_pipe", "Stop Pipeline<br>(No Merge to Main)", { style: "shape=ellipse;fillColor=#f8cecc;strokeColor=#b85450;html=1;whiteSpace=wrap;", w: 160, h: 70 })
  ])
]);

renderTree(d, tree);

// Links
d.link("nyc_data", "nifi", "Call API", { flowAnimation: 1 });
d.link("nifi", "minio", "Put object", { flowAnimation: 1 });
d.link("minio", "nessie", "Trigger", { flowAnimation: 1 });
d.link("nessie", "box_spark", "Checkout dev", { flowAnimation: 1 });
d.link("box_spark", "box_iceberg", "Write branch dev", { flowAnimation: 1 });
d.link("box_iceberg", "gx", "Check Data Quality", { flowAnimation: 1 });
d.link("gx", "merge_main", "PASS", { flowAnimation: 1, rounded: 1 });
d.link("gx", "stop_pipe", "FAIL", { rounded: 1 });
// Route bottom to bottom to avoid crossing the whole diagram and headers
d.link("merge_main", "nessie", "ASSIGN main TO dev", { edgeStyle: "orthogonalEdgeStyle", dashed: 1, rounded: 1, exitX: 0.5, exitY: 1, entryX: 0.5, entryY: 1, labelBackgroundColor: "#ffffff" });

const PROJECT = "/Users/abc/Documents/Project cty";
writeFileSync(`${PROJECT}/data_flow.drawio`, d.mxfile("Data Flow Lakehouse"));
