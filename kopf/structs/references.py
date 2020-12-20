import asyncio
import dataclasses
import enum
import fnmatch
import re
import urllib.parse
from typing import Collection, Iterable, Iterator, List, Mapping, \
                   MutableMapping, NewType, Optional, Pattern, Set, Union

# A namespace specification with globs, negations, and some minimal syntax; see `match_namespace()`.
# Regexps are also supported if pre-compiled from the code, not from the CLI options as raw strings.
NamespacePattern = Union[str, Pattern]

# A specific really existing addressable namespace (at least, the one assumed to be so).
# Made as a NewType for stricter type-checking to avoid collisions with patterns and other strings.
NamespaceName = NewType('NamespaceName', str)

# A namespace reference usable in the API calls. `None` means cluster-wide API calls.
Namespace = Optional[NamespaceName]


def select_specific_namespaces(patterns: Iterable[NamespacePattern]) -> Collection[NamespaceName]:
    """
    Select the namespace specifications that can be used as direct namespaces.

    It is used in a fallback scenario when the namespace observation is either
    disabled or not possible due to restricted permission, while the normal
    operation is still possible in the very specific configured namespaces.
    """
    return {
        NamespaceName(pattern)
        for pattern in patterns
        if isinstance(pattern, str)  # excl. regexps & etc.
        if not('!' in pattern or '*' in pattern or '?' in pattern or ',' in pattern)
    }


def match_namespace(name: NamespaceName, pattern: NamespacePattern) -> bool:
    """
    Check if the specific namespace matches a namespace specification.

    Each individual namespace pattern is a string that follows some syntax:

    * the pattern consists of comma-separated parts (spaces are ignored);
    * each part is either an inclusive or an exclusive (negating) glob;
    * each glob can have ``*`` and ``?`` placeholders for any or one symbols;
    * the exclusive globs start with ``!``;
    * if the the first glob is exclusive, then a preceding catch-all is implied.

    A check of whether a namespace matches the individual pattern, is done by
    iterating the pattern's globs left-to-right: the exclusive patterns exclude
    it from the match; the first inclusive pattern does the initial match, while
    the following inclusive patterns only re-match it if it was excluded before;
    i.e., they do not do the full initial match.

    For example, the pattern ``"myapp-*, !*-pr-*, *pr-123"``
    will match ``myapp-test``, ``myapp-live``, even ``myapp-pr-123``,
    but not ``myapp-pr-456`` and certainly not ``otherapp-pr-123``.
    The latter one, despite it matches the last glob, is not included
    because it was not matched by the initial pattern.

    On the other hand, the pattern ``"!*-pr-*, *pr-123"``
    (equivalent to ``"*, !*-pr-*, *pr-123"``) will match ``myapp-test``,
    ``myapp-live``, ``myapp-pr-123``, ``anyapp-anything``,
    and even ``otherapp-pr-123`` -- though not ``myapp-pr-456``.
    Unlike in the first example, the otherapp's namespace was included initially
    by the first glob (the implied ``*``), and therefore could be re-matched
    by the last glob ``*pr-123`` after being excluded by ``!*-pr-*``.

    While these are theoretical capabilities of this pattern-matching algorithm,
    it is not expected that they will be abused too much. The main intention is
    to have simple one-glob patterns (either inclusive or exclusive),
    only rarely followed by a single negation.
    """

    # Regexps are powerful enough on their own -- we do not parse or interpret them.
    if isinstance(pattern, re.Pattern):
        return bool(pattern.fullmatch(name))

    # The first pattern should be an inclusive one. Unless it is, prepend a catch-all pattern.
    globs = [glob.strip() for glob in pattern.split(',')]
    if not globs or globs[0].startswith('!'):
        globs.insert(0, '*')

    # Iterate and calculate: every inclusive pattern makes the namespace to match regardless,
    # of the previous result; every exclusive pattern un-matches it if it was matched before.
    matches = first_match = fnmatch.fnmatch(name, globs[0])
    for glob in globs[1:]:
        if glob.startswith('!'):
            matches = matches and not fnmatch.fnmatch(name, glob.lstrip('!'))
        else:
            matches = matches or (first_match and fnmatch.fnmatch(name, glob))

    return matches


# Detect conventional API versions for some cases: e.g. in "myresources.v1alpha1.example.com".
# Non-conventional versions are indistinguishable from API groups ("myresources.foo1.example.com").
# See also: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definition-versioning/
K8S_VERSION_PATTERN = re.compile(r'^v\d+(?:(?:alpha|beta)\d+)?$')


@dataclasses.dataclass(frozen=True, eq=False, repr=False)
class Resource:
    """
    A reference to a very specific custom or built-in resource kind.

    It is used to form the K8s API URLs. Generally, K8s API only needs
    an API group, an API version, and a plural name of the resource.
    All other names are remembered to match against resource selectors,
    for logging, and for informational purposes.
    """

    group: str
    version: str
    plural: str

    kind: str
    singular: str
    shortcuts: Collection[str]
    categories: Collection[str]
    subresources: Collection[str]
    namespaced: bool
    preferred: bool
    verbs: Collection[str]

    def __hash__(self) -> int:
        return hash((self.group, self.version, self.plural))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Resource):
            self_tuple = (self.group, self.version, self.plural)
            other_tuple = (other.group, other.version, other.plural)
            return self_tuple == other_tuple
        else:
            return NotImplemented

    def __repr__(self) -> str:
        plural_main, *subs = self.plural.split('/')
        name_text = f'{plural_main}.{self.version}.{self.group}'.strip('.')
        subs_text = f'/{"/".join(subs)}' if subs else ''
        return f'{name_text}{subs_text}'

    @property
    def name(self) -> str:
        return f'{self.plural}.{self.group}'.strip('.')

    @property
    def api_version(self) -> str:
        # Strip heading/trailing slashes if group is absent (e.g. for pods).
        return f'{self.group}/{self.version}'.strip('/')

    def get_url(
            self,
            *,
            server: Optional[str] = None,
            namespace: Namespace = None,
            name: Optional[str] = None,
            subresource: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        if subresource is not None and name is None:
            raise ValueError("Subresources can be used only with specific resources by their name.")

        return self._build_url(server, params, [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
            'namespaces' if namespace is not None else None,
            namespace,
            self.plural,
            name,
            subresource,
        ])

    def get_version_url(
            self,
            *,
            server: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        return self._build_url(server, params, [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
        ])

    def _build_url(
            self,
            server: Optional[str],
            params: Optional[Mapping[str, str]],
            parts: List[Optional[str]],
    ) -> str:
        query = urllib.parse.urlencode(params, encoding='utf-8') if params else ''
        path = '/'.join([part for part in parts if part])
        url = path + ('?' if query else '') + query
        return url if server is None else server.rstrip('/') + '/' + url.lstrip('/')


class Marker(enum.Enum):
    """
    A special marker to handle all resources possible, built-in and custom.
    """
    EVERYTHING = enum.auto()


# An explicit catch-all marker for positional arguments of resource selectors.
EVERYTHING = Marker.EVERYTHING


@dataclasses.dataclass(frozen=True)
class Selector:
    """
    A resource specification that can match several resource kinds.

    The resource specifications are not usable in K8s API calls, as the API
    has no endpoints with masks or placeholders for unknown or catch-all
    resource identifying parts (e.g. any API group, any API version, any name).

    They are used only locally in the operator to match against the actual
    resources with specific names (:class:`Resource`). The handlers are
    defined with resource specifications, but are invoked with specific
    resource kinds. Even if those specifications look very concrete and allow
    no variations, they still remain specifications.
    """

    arg1: dataclasses.InitVar[Union[None, str, Marker]] = None
    arg2: dataclasses.InitVar[Union[None, str, Marker]] = None
    arg3: dataclasses.InitVar[Union[None, str, Marker]] = None
    argN: dataclasses.InitVar[None] = None  # a runtime guard against too many positional arguments

    group: Optional[str] = None
    version: Optional[str] = None

    kind: Optional[str] = None
    plural: Optional[str] = None
    singular: Optional[str] = None
    shortcut: Optional[str] = None
    category: Optional[str] = None
    any_name: Optional[Union[str, Marker]] = None

    def __post_init__(
            self,
            arg1: Union[None, str, Marker],
            arg2: Union[None, str, Marker],
            arg3: Union[None, str, Marker],
            argN: None,  # a runtime guard against too many positional arguments
    ) -> None:

        # Since the class is frozen & read-only, post-creation field adjustment is done via a hack.
        # This is the same hack as used in the frozen dataclasses to initialise their fields.
        if argN is not None:
            raise TypeError("Too many positional arguments. Max 3 positional args are accepted.")
        elif arg3 is not None:
            object.__setattr__(self, 'group', arg1)
            object.__setattr__(self, 'version', arg2)
            object.__setattr__(self, 'any_name', arg3)
        elif arg2 is not None and isinstance(arg1, str) and '/' in arg1:
            object.__setattr__(self, 'group', arg1.rsplit('/', 1)[0])
            object.__setattr__(self, 'version', arg1.rsplit('/')[-1])
            object.__setattr__(self, 'any_name', arg2)
        elif arg2 is not None and arg1 == 'v1':
            object.__setattr__(self, 'group', '')
            object.__setattr__(self, 'version', arg1)
            object.__setattr__(self, 'any_name', arg2)
        elif arg2 is not None:
            object.__setattr__(self, 'group', arg1)
            object.__setattr__(self, 'any_name', arg2)
        elif arg1 is not None and isinstance(arg1, Marker):
            object.__setattr__(self, 'any_name', arg1)
        elif arg1 is not None and '.' in arg1 and K8S_VERSION_PATTERN.match(arg1.split('.')[1]):
            if len(arg1.split('.')) >= 3:
                object.__setattr__(self, 'group', arg1.split('.', 2)[2])
            object.__setattr__(self, 'version', arg1.split('.')[1])
            object.__setattr__(self, 'any_name', arg1.split('.')[0])
        elif arg1 is not None and '.' in arg1:
            object.__setattr__(self, 'group', arg1.split('.', 1)[1])
            object.__setattr__(self, 'any_name', arg1.split('.')[0])
        elif arg1 is not None:
            object.__setattr__(self, 'any_name', arg1)

        # Verify that explicit & interpreted arguments have produced an unambiguous specification.
        names = [self.kind, self.plural, self.singular, self.shortcut, self.category, self.any_name]
        clean = [name for name in names if name is not None]
        if len(clean) > 1:
            raise TypeError(f"Ambiguous resource specification with names {clean}")
        if len(clean) < 1:
            raise TypeError(f"Unspecific resource with no names.")

        # For reasons unknown, the singular is empty for ALL builtin resources. This does not affect
        # the checks unless defined as e.g. ``singular=""``, which would match ALL builtins at once.
        # Thus we prohibit it until clarified why is it so, what does it mean, how to deal with it.
        if any([name == '' for name in names]):
            raise TypeError("Names must not be empty strings; either None or specific strings.")

    def __repr__(self) -> str:
        kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        kwtext = ', '.join([f'{key!s}={val!r}' for key, val in kwargs.items() if val is not None])
        clsname = self.__class__.__name__
        return f'{clsname}({kwtext})'

    @property
    def is_specific(self) -> bool:
        return (self.kind is not None or
                self.shortcut is not None or
                self.plural is not None or
                self.singular is not None or
                (self.any_name is not None and not isinstance(self.any_name, Marker)))

    def check(
            self,
            resource: Resource,
    ) -> bool:
        """
        Check if a specific resources matches this resource specification.
        """
        return (
            (self.group is None or self.group == resource.group) and
            ((self.version is None and resource.preferred) or self.version == resource.version) and
            (self.kind is None or self.kind == resource.kind) and
            (self.plural is None or self.plural == resource.plural) and
            (self.singular is None or self.singular == resource.singular) and
            (self.category is None or self.category in resource.categories) and
            (self.shortcut is None or self.shortcut in resource.shortcuts) and
            (self.any_name is None or
             self.any_name is Marker.EVERYTHING or
             self.any_name == resource.kind or
             self.any_name == resource.plural or
             self.any_name == resource.singular or
             self.any_name in resource.shortcuts))

    def select(
            self,
            resources: Collection[Resource],
            *,
            all: bool,
    ) -> Collection[Resource]:
        result = {resource for resource in resources if self.check(resource)}

        # Core v1 API group's priority is hard-coded in K8s and kubectl. Do the same. For example:
        # whenever "pods" is specified, and "pods.v1" & "pods.v1beta1.metrics.k8s.io" are found,
        # implicitly give priority to "v1" and hide the existence of non-"v1" groups.
        # But not if they are specified by categories! -- In that case, keep all resources as is.
        if self.is_specific and not all:
            v1only = {resource for resource in result if resource.group == ''}
            result = v1only or result

        return result


# Some predefined API endpoints that we use in the framework itself (not exposed to the operators).
# Note: the CRDs are versionless: we do not look into its ``spec`` stanza, we only watch for
# the fact of changes, so the schema does not matter, any cluster-preferred API version would work.
CRDS = Selector('apiextensions.k8s.io', 'customresourcedefinitions')
EVENTS = Selector('v1', 'events')
NAMESPACES = Selector('v1', 'namespaces')
CLUSTER_PEERINGS = Selector('zalando.org/v1', 'clusterkopfpeerings')
NAMESPACED_PEERINGS = Selector('zalando.org/v1', 'kopfpeerings')


class Backbone(Mapping[Selector, Resource]):
    """
    Actual resources used in the core (reactor & engines) of the framework.

    Why? The codebase only refers to the resources by API group/version & names.
    The actual resources can be different in different clusters, usually due
    to different versions: e.g. "v1" vs. "v1beta1" for CRDs.

    The actual backbone resources are detected in the initial cluster scanning
    during the operator startup in :func:`resource_scanner`.

    The backbone resources cannot be changed at runtime after they are found
    for the first time -- since the core tasks are already started with those
    resource definitions, and cannot be easily restarted.

    This does not apply to the resources of the operator (not the framework!),
    where the resources can be created, changed, and deleted at runtime easily.
    """

    def __init__(self) -> None:
        super().__init__()
        self._items: MutableMapping[Selector, Resource] = {}
        self.revised = asyncio.Condition()

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[Selector]:
        return iter(self._items)

    def __getitem__(self, item: Selector) -> Resource:
        return self._items[item]

    def fill(
            self,
            *,
            resources: Iterable[Resource],
    ) -> None:
        for resource in resources:
            for spec in [NAMESPACES, EVENTS, CRDS, CLUSTER_PEERINGS, NAMESPACED_PEERINGS]:
                if spec not in self._items:
                    if spec.check(resource):
                        self._items[spec] = resource


@dataclasses.dataclass(frozen=True)
class Insights:
    """
    Actual resources & namespaces served by the operator.
    """
    namespaces: Set[Namespace] = dataclasses.field(default_factory=set)
    resources: Set[Resource] = dataclasses.field(default_factory=set)
    backbone: Backbone = dataclasses.field(default_factory=Backbone)

    # Signalled when anything changes in the insights.
    revised: asyncio.Condition = dataclasses.field(default_factory=asyncio.Condition)

    # The flags that are set after the initial listing is finished. Not cleared afterwards.
    ready_namespaces: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    ready_resources: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.backbone.revised = self.revised
