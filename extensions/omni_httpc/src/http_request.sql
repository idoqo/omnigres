create function http_request(
    url text,
    method omni_http.http_method default 'GET',
    headers omni_http.http_headers default array []::omni_http.http_headers,
    body bytea default null,
    follow_redirect boolean default false) returns http_request as
$$
select row (method, url, headers, body)
$$
    language sql
    immutable;