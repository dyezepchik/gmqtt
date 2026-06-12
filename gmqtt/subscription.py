from typing import Sequence, Union


class Subscription:
    def __init__(
        self,
        topic,
        qos=0,
        no_local=False,
        retain_as_published=False,
        retain_handling_options=0,
        subscription_identifier=None,
    ):
        self.topic = topic
        self.qos = qos
        self.no_local = no_local
        self.retain_as_published = retain_as_published
        self.retain_handling_options = retain_handling_options

        self.mid = None
        self.acknowledged = False

        # this property can be used only in MQTT5.0
        self.subscription_identifier = subscription_identifier


class SubscriptionsHandlerMixin:
    def __init__(self):
        self.subscriptions: list = []
        # defined by a main class
        self._connection = None

    def update_subscriptions_with_subscription_or_topic(
        self,
        subscription_or_topic,
        qos,
        no_local,
        retain_as_published,
        retain_handling_options,
        kwargs,
    ):

        sentinel = object()
        subscription_identifier = kwargs.get("subscription_identifier", sentinel)

        if isinstance(subscription_or_topic, Subscription):

            if subscription_identifier is not sentinel:
                subscription_or_topic.subscription_identifier = subscription_identifier

            subscriptions = [subscription_or_topic]
        elif isinstance(subscription_or_topic, (tuple, list)):

            if subscription_identifier is not sentinel:
                for sub in subscription_or_topic:
                    sub.subscription_identifier = subscription_identifier

            subscriptions = subscription_or_topic
        elif isinstance(subscription_or_topic, str):

            if subscription_identifier is sentinel:
                subscription_identifier = None

            subscriptions = [
                Subscription(
                    subscription_or_topic,
                    qos=qos,
                    no_local=no_local,
                    retain_as_published=retain_as_published,
                    retain_handling_options=retain_handling_options,
                    subscription_identifier=subscription_identifier,
                )
            ]
        else:
            raise ValueError(
                "Bad subscription: must be string or Subscription or list of Subscriptions"
            )
        self.subscriptions.extend(subscriptions)
        return subscriptions

    def _remove_subscriptions(self, topic: Union[str, Sequence[str]]):
        if isinstance(topic, str):
            self.subscriptions = [s for s in self.subscriptions if s.topic != topic]
        else:
            self.subscriptions = [s for s in self.subscriptions if s.topic not in topic]

    def subscribe(
        self,
        subscription_or_topic: Union[str, Subscription, Sequence[Subscription]],
        qos=0,
        no_local=False,
        retain_as_published=False,
        retain_handling_options=0,
        **kwargs
    ):

        # Warn: if you will pass a few subscriptions objects, and each will be have different
        # subscription identifier - the only first will be used as identifier
        # if only you will not pass the identifier in kwargs

        subscriptions = self.update_subscriptions_with_subscription_or_topic(
            subscription_or_topic,
            qos,
            no_local,
            retain_as_published,
            retain_handling_options,
            kwargs,
        )
        return self._connection.subscribe(subscriptions, **kwargs)

    def resubscribe(self, subscription: Subscription, **kwargs):
        # send subscribe packet for subscription,that's already in client's subscription list
        if "subscription_identifier" in kwargs:
            subscription.subscription_identifier = kwargs["subscription_identifier"]
        elif subscription.subscription_identifier is not None:
            kwargs["subscription_identifier"] = subscription.subscription_identifier
        return self._connection.subscribe([subscription], **kwargs)

    def unsubscribe(self, topic: Union[str, Sequence[str]], **kwargs):
        self._remove_subscriptions(topic)
        return self._connection.unsubscribe(topic, **kwargs)

