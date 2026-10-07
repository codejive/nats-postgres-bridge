CREATE TABLE public.messages (
    time timestamptz NOT NULL,
    topic text NOT NULL,
    value text
);
CREATE INDEX messages_topic_time_idx ON public.messages (topic, time);
