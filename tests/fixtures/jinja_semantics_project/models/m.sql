-- depends_on: {{ ref('a') }}
select 'see {{ ref("b") }}' as s /* {{ ref('c2') }} */ {# {{ ref('d') }} #}
