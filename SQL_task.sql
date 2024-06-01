WITH closed_times AS (
    SELECT DISTINCT ON (ticket_id) id, ticket_id, state_id, created_time, change_time
    FROM ticket_history
    ORDER BY change_time DESC
),
number_of_messages AS (
    SELECT ticket_id, count(id) as emails_count
    FROM article
    GROUP BY ticket_id
),
ticket_description AS (
	SELECT id, value
    FROM dynamic_field_value
    WHERE field_id = 19
)
SELECT
    ticket.tn       as ticket_id,
    ticket.title    as title,
	closed_times.created_time as opened,
	closed_times.change_time as last_change,
	CASE
        WHEN closed_times.state_id in (2, 3) THEN closed_times.change_time
        ELSE NULL
    END as close_time,
	queue.name as queue_name,
    ticket_state.name as ticket_state,
	ticket_priority.name as ticket_priority,
	customer_user.email as client_email,
	customer_user.customer_id as client_id,
	ticket_type.name as request_type,
	number_of_messages.emails_count as emails_count
	ticket_description.value as request_description
FROM
    ticket
	LEFT JOIN ticket_state
        ON ticket.ticket_state_id = ticket_state.id
    LEFT JOIN closed_times
        ON closed_times.ticket_id = ticket.id
    LEFT JOIN customer_user
	    ON ticket.customer_id = customer_user.customer_id
    LEFT JOIN number_of_messages
	    ON ticket.id = number_of_messages.ticket_id
    LEFT JOIN ticket_description
	    ON ticket.id = ticket_description.id
    LEFT JOIN ticket_priority
	    ON ticket.ticket_priority_id = ticket_priority.id
    LEFT JOIN queue
	    ON ticket.queue_id = queue.id
    LEFT JOIN ticket_type
        ON ticket.type_id = ticket_type.id
WHERE
    ticket.create_time >= CURRENT_DATE - INTERVAL ‘30 days’
    AND ticket.ticket_state_id NOT IN (5, 9)
    AND ticket.queue_id IN (2, 3, 6, 8, 9, 10, 11);
