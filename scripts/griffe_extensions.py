from griffe import Class, DocstringParameter, DocstringSectionParameters, Extension


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
