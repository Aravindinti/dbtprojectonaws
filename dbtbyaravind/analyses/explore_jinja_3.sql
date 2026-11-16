{% set inc_flag=1%}
{% set last_load='2000-01-01'%}
{% set cols_list=['status', 'user_id','order_date', 'id']%}
select {%- for col in cols_list %}
         {{ col }}{%- if not loop.last %},
          {%- endif %}
          {%- endfor %} from {{ref("bronze_orders")}} 
{% if inc_flag == 1 %} 
    where order_date >= '{{ last_load }}'
{% endif %} 