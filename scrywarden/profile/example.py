from scrywarden.profile import fields, Profile, reporters
from scrywarden.transport.message import Message


class ExampleProfile(Profile):
    """Example profile used for testing and demonstration purposes.

    Matches well with the default heartbeat transports.
    """

    greeting = fields.Single(key='greeting', reporter=reporters.Mandatory())

    def matches(self, message: Message) -> bool:
        return 'greeting' in message

    def get_actor(self, message: Message) -> str:
        return message.get('person')
