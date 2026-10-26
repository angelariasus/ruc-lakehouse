"""Keep the Terraform configuration in step with the code it deploys."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
INFRA = ROOT / "infra"


def variable_default(name: str) -> str:
    variables = (INFRA / "variables.tf").read_text(encoding="utf-8")
    block = re.search(rf'variable "{name}" {{(.*?)\n}}', variables, flags=re.DOTALL)
    assert block, f"variable {name} not found"
    default = re.search(r'default\s*=\s*"([^"]+)"', block.group(1))
    assert default, f"variable {name} has no string default"
    return default.group(1)


def terraform_list(name: str) -> set[str]:
    main = (INFRA / "main.tf").read_text(encoding="utf-8")
    match = re.search(rf"{name}\s*=\s*\[([^\]]*)\]", main)
    assert match, f"local {name} not found"
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def test_job_image_matches_the_emr_release():
    dockerfile = (ROOT / "docker" / "emr" / "Dockerfile").read_text(encoding="utf-8")
    image_release = re.search(r"^ARG EMR_RELEASE=(\S+)$", dockerfile, flags=re.MULTILINE)
    assert image_release
    assert image_release.group(1) == variable_default("emr_release_label")


def test_glue_databases_cover_the_spark_namespaces():
    sources = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "src").rglob("*.py"))
    namespaces = set(re.findall(r"\{CATALOG\}\.(\w+)\.\w+", sources))
    namespaces.discard("smoke")  # local check only, never deployed
    assert namespaces == terraform_list("job_databases")


def test_glue_databases_cover_the_dbt_schemas():
    project = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))
    schemas = {
        config["+schema"]
        for section in ("models", "seeds")
        for config in project[section]["ruc_lakehouse"].values()
        if isinstance(config, dict) and "+schema" in config
    }
    schemas.add(project["seeds"]["ruc_lakehouse"]["+schema"])
    assert schemas == terraform_list("transform_databases")
