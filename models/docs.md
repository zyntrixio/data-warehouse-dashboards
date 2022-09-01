
[!--    The docs.md file contains docs blocks that can be referenced from within or outside of the project.
        For example, the __overview__ section will be shown as soon as you open project dcumentation site.
        
        If a description will be used repeatedly, you might choose to define it here and simply reference it
        in the relevent description (see description_example ).
--]

{% docs __overview__ %}
# dbt_template
This dbt project is a template used for creating other projects for Lumilinks.

All of the objects that should be needed will have been created in the correct structure.
As a deveoper, you should only need to update and / or replace the relevent files as necessary.

The following objects will exist in every project and should be updated to match your solution:

    - dbt_project.yml
    - packages.yml
    
    - macros\macros.yml
    - models\docs.md
    - models\exposures.yml

These objects will need to be replaced with objects relevant to your project:

    - marts\core\core.yml
    - marts\core\[CoreDimensions].sql

    - marts\[MyMart]
    - marts\[MyMart]\[MyMart].yml
    - marts\[MyMart]\[MyMartDimensions].sql

    - staging\[Source]
    - staging\[Source]\[Source].yml
    - staging\[Source]\[Source__Tables].sql
    - staging\[Source]\[Transform].yml

    - transform\[Transformation].sql
    - transform\trans.yml

    - tests\[ProjectSpecificTests].sql

These objects are common across all Lumilinks projects and are unlikely to be updated from
one project to the next:

    - macro\generate_schema_names.sql
    - macro\get_source_select.sql

    - models\elementary

More information can be found in the data engineer confluence pages: https://lumilinks.atlassian.net/wiki/spaces/DE/overview

{% enddocs %}


{% docs description_example %}

This is an example of a description that I can reference from elsewhere within my project.
In this case, I will call it from mart1.yml.
It's very good for when the same description will be used multiple times (e.g. where the same column is used over an over again).

{% enddocs %}