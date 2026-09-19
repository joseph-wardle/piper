"""The contexts this studio works in: disciplines, each authored in one host."""

from dataclasses import dataclass

from piper.errors import PiperError


@dataclass(frozen=True, slots=True)
class Context:
    """A discipline work is done in, distinct from any tracker task.

    ``subject`` is the kind of entity the work is about, ``host`` the application
    it is authored in, and ``extension`` the work file's, without its dot.
    """

    name: str
    subject: str
    host: str
    extension: str


CONTEXTS = (Context(name="modeling", subject="asset", host="maya", extension="mb"),)


def context_named(name: str, *, subject: str) -> Context:
    """The context called ``name`` for a kind of entity, refusing a name that is not one."""
    offered = [context for context in CONTEXTS if context.subject == subject]
    for context in offered:
        if context.name == name:
            return context
    names = ", ".join(context.name for context in offered) or "none"
    raise PiperError(f"no {subject} context is named {name!r} (contexts: {names})")
