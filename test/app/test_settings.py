def test_broker_connection_string(default_settings):
    assert default_settings.broker_connection_string == 'amqp://test_user:test_pw@test-host:1234/'


def test_backend_connection_string(default_settings):
    assert (
        default_settings.backend_connection_string
        == 'db+postgresql://test_user:test_password@test-host:1234/test_database'
    )
