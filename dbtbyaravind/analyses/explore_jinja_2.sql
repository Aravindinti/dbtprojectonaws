{%-set apples=["Fuji","Gala","Honeycrisp"]-%}
{% for i in apples%}
{%-if i != "Honeycrisp"-%}
{{i}}

{%else%}
i hate {{i}} because it is very costly 
{%-endif-%}
{% endfor %}