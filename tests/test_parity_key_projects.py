"""Request project defaults used by clients that omit projects from keys."""

import pytest
from google.cloud.datastore_v1.types import (
    AllocateIdsRequest,
    ArrayValue,
    CommitRequest,
    CompositeFilter,
    Entity,
    Filter,
    Key,
    KindExpression,
    LookupRequest,
    Mutation,
    PartitionId,
    PropertyFilter,
    PropertyReference,
    Query,
    ReadOptions,
    RunQueryRequest,
    TransactionOptions,
)
from parity_common import (  # noqa: F401
    assert_parity,
    commit_raw,
    google_client,
    outcome,
    request_ids,
    rust_client,
    unique_kind,
)


@pytest.fixture
def kind():
    return unique_kind("KeyProject")


def _key(client, kind, shape="absent", name="item"):
    key = Key(path=[Key.PathElement(kind=kind, name=name)])
    if shape == "empty":
        key.partition_id = PartitionId()
    elif shape == "namespace":
        key.partition_id = PartitionId(namespace_id="isolated")
    elif shape == "explicit":
        key.partition_id = PartitionId(project_id=client.project)
    return key


def _both(rust, google, kind, scenario, expected):
    results = []
    for client in (google, rust):
        try:
            results.append(outcome(lambda: scenario(client)))
        finally:
            for namespace in (None, "isolated"):
                query = client.query(kind=kind, namespace=namespace)
                query.keys_only()
                client.delete_multi([entity.key for entity in query.fetch()])
    assert_parity(results[0], results[1], kind)
    assert results[0] == ("ok", expected)


def _lookup(client, keys, **kwargs):
    return client._datastore_api.lookup(
        request=LookupRequest(**request_ids(client), keys=keys, **kwargs)
    )


@pytest.mark.parametrize("shape", ["absent", "empty", "namespace"])
def test_commit_key_defaults_are_visible_to_queries(rust_client, google_client, kind, shape):
    def scenario(client):
        key = _key(client, kind, shape)
        commit_raw(client, [Mutation(upsert=Entity(key=key))])
        response = client._datastore_api.run_query(
            request=RunQueryRequest(
                **request_ids(client),
                partition_id=PartitionId(namespace_id=key.partition_id.namespace_id),
                query=Query(kind=[KindExpression(name=kind)]),
            )
        )
        return [
            (item.entity.key.partition_id.project_id == client.project,
             item.entity.key.partition_id.namespace_id)
            for item in response.batch.entity_results
        ]

    _both(rust_client, google_client, kind, scenario, [(True, "isolated" if shape == "namespace" else "")])


@pytest.mark.parametrize("shape", ["absent", "empty"])
def test_lookup_missing_key_projects_match_explicit_keys(rust_client, google_client, kind, shape):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=Entity(key=_key(client, kind, "explicit")))])
        response = _lookup(client, [_key(client, kind, shape)])
        return [item.entity.key.partition_id.project_id == client.project for item in response.found]

    _both(rust_client, google_client, kind, scenario, [True])


def test_transaction_lookup_defaults_the_key_project(rust_client, google_client, kind):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=Entity(key=_key(client, kind, "explicit")))])
        transaction = client._datastore_api.begin_transaction(request=request_ids(client)).transaction
        try:
            response = _lookup(
                client, [_key(client, kind)], read_options=ReadOptions(transaction=transaction)
            )
            return [item.entity.key.partition_id.project_id == client.project for item in response.found]
        finally:
            client._datastore_api.rollback(request=dict(**request_ids(client), transaction=transaction))

    _both(rust_client, google_client, kind, scenario, [True])


@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
def test_mutation_keys_share_the_request_project(rust_client, google_client, kind, operation):
    def scenario(client):
        explicit = _key(client, kind, "explicit")
        if operation != "insert":
            commit_raw(client, [Mutation(upsert=Entity(key=explicit))])
        bare = _key(client, kind)
        mutation = Mutation(**{
            operation: bare if operation == "delete" else Entity(
                key=bare, properties={"value": {"integer_value": 7}}
            )
        })
        commit_raw(client, [mutation])
        response = _lookup(client, [explicit])
        return [item.entity.properties["value"].integer_value for item in response.found]

    _both(rust_client, google_client, kind, scenario, [] if operation == "delete" else [7])


def test_projects_are_defaulted_in_nested_values(rust_client, google_client, kind):
    def scenario(client):
        bare = _key(client, kind, name="reference")
        foreign = Key(bare)
        foreign.partition_id = PartitionId(project_id="foreign-project", namespace_id="foreign")
        entity = Entity(key=_key(client, kind), properties={
            "ref": {"key_value": bare},
            "refs": {"array_value": ArrayValue(values=[{"key_value": bare}, {"key_value": foreign}])},
            "embedded": {"entity_value": Entity(
                key=bare, properties={"ref": {"key_value": bare}}
            )},
            "unkeyed": {"entity_value": Entity()},
        })
        commit_raw(client, [Mutation(upsert=entity)])
        stored = _lookup(client, [_key(client, kind, "explicit")]).found[0].entity
        props = stored.properties
        return {
            "root": stored.key.partition_id.project_id == client.project,
            "ref": props["ref"].key_value.partition_id.project_id == client.project,
            "array": [v.key_value.partition_id.project_id for v in props["refs"].array_value.values],
            "embedded": props["embedded"].entity_value.key.partition_id.project_id == client.project,
            "nested": props["embedded"].entity_value.properties["ref"].key_value.partition_id.project_id == client.project,
            "empty": props["unkeyed"].entity_value.key.partition_id.project_id,
        }

    # Reduce the contextual project to a flag in the array, preserving foreign projects.
    def normalized(client):
        result = scenario(client)
        result["array"] = [p == client.project if p != "foreign-project" else p for p in result["array"]]
        return result

    _both(rust_client, google_client, kind, normalized, {
        "root": True, "ref": True, "array": [True, "foreign-project"],
        "embedded": True, "nested": True, "empty": "",
    })


def _filtered_query(client, kind):
    return Query(kind=[KindExpression(name=kind)], filter=Filter(
        composite_filter=CompositeFilter(op=CompositeFilter.Operator.AND, filters=[
            Filter(property_filter=PropertyFilter(
                property=PropertyReference(name="__key__"), op=PropertyFilter.Operator.EQUAL,
                value={"key_value": _key(client, kind)},
            )),
            Filter(property_filter=PropertyFilter(
                property=PropertyReference(name="ref"), op=PropertyFilter.Operator.EQUAL,
                value={"key_value": _key(client, kind, name="reference")},
            )),
        ])
    ))


def test_query_filter_key_values_use_the_request_project(rust_client, google_client, kind):
    def scenario(client):
        entity = Entity(key=_key(client, kind, "explicit"), properties={
            "ref": {"key_value": _key(client, kind, "explicit", name="reference")}
        })
        commit_raw(client, [Mutation(upsert=entity)])
        query = _filtered_query(client, kind)
        response = client._datastore_api.run_query(
            request=RunQueryRequest(**request_ids(client), query=query)
        )
        return len(response.batch.entity_results)

    _both(rust_client, google_client, kind, scenario, 1)


def test_transaction_insert_and_update_share_a_normalized_key(rust_client, google_client, kind):
    def scenario(client):
        commit_raw(client, [
            Mutation(insert=Entity(key=_key(client, kind))),
            Mutation(update=Entity(
                key=_key(client, kind, "explicit"), properties={"value": {"integer_value": 7}}
            )),
        ], mode=CommitRequest.Mode.TRANSACTIONAL, single_use_transaction=TransactionOptions(
            read_write=TransactionOptions.ReadWrite()
        ))
        response = _lookup(client, [_key(client, kind, "explicit")])
        return [item.entity.properties["value"].integer_value for item in response.found]

    _both(rust_client, google_client, kind, scenario, [7])


def test_allocate_ids_normalizes_the_returned_key_project(rust_client, google_client, kind):
    def scenario(client):
        response = client._datastore_api.allocate_ids(request=AllocateIdsRequest(
            **request_ids(client), keys=[Key(path=[Key.PathElement(kind=kind)])],
        ))
        key = response.keys[0]
        return key.partition_id.project_id == client.project, key.path[-1].id > 0

    _both(rust_client, google_client, kind, scenario, (True, True))
