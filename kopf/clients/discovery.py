from typing import Dict, Optional, cast

from kopf.clients import auth, errors
from kopf.structs import references


@auth.reauthenticated_request
async def discover(
        *,
        resource: references.Resource,
        subresource: Optional[str] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[Dict[str, object]]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    name = resource.plural if subresource is None else f'{resource.plural}/{subresource}'

    if not context._discovered_resources.get(resource.api_version, {}).get(name):
        async with context._discovery_lock:
            if resource.api_version not in context._discovered_resources:
                context._discovered_resources[resource.api_version] = {}

                try:
                    url = resource.get_version_url(server=context.server)
                    rsp = await errors.parse_response(await context.session.get(url))

                    context._discovered_resources[resource.api_version].update({
                        info['name']: info
                        for info in rsp['resources']
                    })

                except (errors.APINotFoundError, errors.APIForbiddenError):
                    pass

    return context._discovered_resources[resource.api_version].get(name, None)


@auth.reauthenticated_request
async def is_namespaced(
        *,
        resource: references.Resource,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> bool:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    info = await discover(resource=resource, context=context)
    return cast(bool, info['namespaced']) if info is not None else True  # assume namespaced


@auth.reauthenticated_request
async def is_status_subresource(
        *,
        resource: references.Resource,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> bool:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    info = await discover(resource=resource, subresource='status', context=context)
    return info is not None
