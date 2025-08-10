from typing import Any

from griffe import (
    Attribute,
    Class,
    DocstringParameter,
    DocstringSectionParameters,
    ExprName,
    Extension,
)


class PropertyFieldExtension(Extension):
    def on_class_members(self, node, cls: Class, agent, **kwargs):
        properties = {
            k: v for k, v in cls.attributes.items() if v.has_labels("property")
        }
        if not properties:
            return

        if cls.docstring and cls.docstring.parsed:
            parameters = [
                DocstringParameter(
                    name=k,
                    description="*(computed property)* "
                    + (v.docstring.value if v.docstring else ""),
                    annotation=v.annotation,
                )
                for k, v in properties.items()
            ]

            parameters_section = next(
                (
                    section
                    for section in cls.docstring.parsed
                    if isinstance(section, DocstringSectionParameters)
                ),
                None,
            )
            if not parameters_section:
                parameters_section = DocstringSectionParameters(value=[])
                cls.docstring.parsed.append(parameters_section)

            parameters_section.value.extend(parameters)

            # Remove properties from cls.members so they don't get separate sections
            for name in properties:
                cls.members.pop(name, None)


class RenameParametersSectionForDataclasses(Extension):
    def on_class_instance(self, node, cls: Class, agent, **kwargs):
        if not cls.has_labels or not cls.has_labels("dataclass"):
            return

        if not cls.docstring or not cls.docstring.parsed:
            return

        for section in cls.docstring.parsed:
            if (
                isinstance(section, DocstringSectionParameters)
                and section.title is None
            ):
                section.title = "Fields:"


ENUM_BASES = {"Enum", "IntEnum", "StrEnum", "Flag", "IntFlag"}


def _is_enum_class(cls: Class) -> bool:
    if cls.name == "ArchiveFormat":
        print("hehe")

    # Any base canonical path matching Python enums
    if any(getattr(b, "canonical_name", None) in ENUM_BASES for b in cls.bases or ()):
        return True

    flag = cls.members.get("__enum_like__")
    if isinstance(flag, Attribute) and flag.value in (True, "True"):
        return True

    return False


class EnumMembersAsTable(Extension):
    # The existing Griffe docstrings sections are not appropriate for enum classes.
    # The closest would be DocstringSectionOtherParameters, but it renders a "Type"
    # column instead of a "Value" one. So here we add the enum members to the extra
    # information, and override the class template
    # (see docs_templates/python/material/class.html.jinja)
    # to add a new fragment that renders that info as a table.

    def on_class_members(self, node, cls: Class, agent, **kwargs):
        if not _is_enum_class(cls):
            return
        rows: list[dict[str, Any]] = []
        for name, m in list(cls.members.items()):
            if m.kind.value == "attribute" and not name.startswith("_"):
                assert isinstance(m, Attribute)
                # The second condition is to handle ArchiveFormat, which is not an
                # enum class, but has class variables that are like enum values.
                # The rendering is not perfect as the values are empty, but it's
                # better than nothing.
                if (m.value is not None and m.annotation is None) or (
                    isinstance(m.annotation, ExprName)
                    and m.annotation.canonical_path == cls.canonical_path
                ):
                    rows.append(
                        {
                            "name": name,
                            "value": m.value,
                            "doc": (m.docstring.value if m.docstring else ""),
                        }
                    )
                    cls.members.pop(name, None)
        if rows:
            cls.extra.setdefault("enum_members", {"rows": rows})
