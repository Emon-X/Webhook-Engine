
def create_source(client, name):
    response = client.post(
        "/sources/",
        json={
            "name": name,
            "signing_secret" : "test-secret-value-123456"
        }
    )

    assert response.status_code == 200
    return response.json()


def test_source_pagination(client):
    # Arrange: create three sources.
    create_source(client, "source-one")
    create_source(client, "source-two")
    create_source(client, "source-three")

    # Act: request the first page.
    response = client.get("/sources/?page=1&limit=2")

    # Assert.
    assert response.status_code == 200

    data = response.json()

    assert len(data["items"]) == 2
    assert data["total"] == 3
    assert data["page"] == 1
    assert data["limit"] == 2
    assert data["pages"] == 2

    # Request the second page.
    response = client.get("/sources/?page=2&limit=2")

    data = response.json()

    assert response.status_code == 200
    assert len(data["items"]) == 1
    assert data["page"] == 2


def test_filter_active_sources(client):
    create_source(client, "active-source")
    inactive_source = create_source(client, "inactive-source")

    # Deactivate the second source.
    response = client.delete(
        f"/sources/{inactive_source['id']}"
    )

    assert response.status_code in (200, 204)

    # Retrieve active sources.
    response = client.get("/sources/?is_active=true")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 1
    assert all(
        source["is_active"] is True
        for source in data["items"]
    )

    # Retrieve inactive sources.
    response = client.get("/sources/?is_active=false")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 1
    assert all(
        source["is_active"] is False
        for source in data["items"]
    )


def test_invalid_pagination(client):
    # Page cannot be zero.
    response = client.get("/sources/?page=0")

    assert response.status_code == 422

    # Limit cannot exceed 100.
    response = client.get("/sources/?limit=101")

    assert response.status_code == 422
    
    
def test_create_source(client):
    response = client.post(
        "/sources/",
        json={"name": "new-source", "signing_secret" : "test-secret-value-123456"}
    )

    assert response.status_code in (200, 201)

    data = response.json()

    assert data["name"] == "new-source"
    assert data["is_active"] is True
    assert "id" in data
    
def test_duplicate_source(client):
    create_source(client, "duplicate-source")

    response = client.post(
        "/sources/",
        json={"name": "duplicate-source", "signing_secret" : "test-secret-value-123456"}
    )

    assert response.status_code == 409
    
def test_get_source(client):
    source = create_source(client, "retrieve-source")

    response = client.get(
        f"/sources/{source['id']}"
    )

    assert response.status_code == 200
    assert response.json()["name"] == "retrieve-source"
    
def test_get_nonexistent_source(client):
    response = client.get(
        "/sources/00000000-0000-0000-0000-000000000000"
    )

    assert response.status_code == 404
    
def test_update_source(client):
    source = create_source(client, "old-name")

    response = client.patch(
        f"/sources/{source['id']}",
        json={"name": "new-name","signing_secret" : "test-secret-value-123456"}
    )

    assert response.status_code == 200
    assert response.json()["name"] == "new-name"
    
def test_deactivate_source(client):
    source = create_source(client, "deactivate-source")

    response = client.delete(
        f"/sources/{source['id']}"
    )

    assert response.status_code in (200, 204)

    response = client.get(
        f"/sources/{source['id']}"
    )

    assert response.status_code == 200
    assert response.json()["is_active"] is False
    
def test_create_source_without_secret(client):
    response = client.post(
        "/sources/",
        json={"name": "missing-secret-source"}
    )

    assert response.status_code == 422
    
def test_source_response_does_not_expose_secret(client):
    response = client.post(
        "/sources/",
        json={
            "name": "secure-source",
            "signing_secret": "test-secret-value-123456"
        }
    )

    assert response.status_code in (200, 201)

    data = response.json()

    assert "signing_secret" not in data
    assert "signing_secret_encrypted" not in data