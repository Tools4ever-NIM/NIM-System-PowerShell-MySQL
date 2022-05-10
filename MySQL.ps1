#
# MySQL.ps1 - IDM System PowerShell Script for Microsoft SQL Server.
#
# Any IDM System PowerShell Script is dot-sourced in a separate PowerShell context, after
# dot-sourcing the IDM Generic PowerShell Script '../Generic.ps1'.
#


$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'server'
                type = 'textbox'
                label = 'Server'
                description = 'Name of Microsoft SQL server'
                value = ''
            }
            @{
                name = 'ssl_mode'
                type = 'checkbox'
                label = 'SSL Mode Enabled'
                description = 'Enable SSL Mode'
                value = $false
            }
            @{
                name = 'database'
                type = 'textbox'
                label = 'Database'
                description = 'Name of Microsoft SQL database'
                value = ''
            }
            @{
                name = 'username'
                type = 'textbox'
                label = 'Username'
                label_indent = $true
                description = 'User account name to access Microsoft SQL server'
                value = ''
                hidden = 'use_svc_account_creds'
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                description = 'User account password to access Microsoft SQL server'
                value = ''
                hidden = 'use_svc_account_creds'
            }
            @{
                name = 'mysql_net_installpath'
                type = 'textbox'
                label = 'MySQL .NET installation path'
                description = 'Path of MySQL .NET installation'
                value = 'C:\Program Files (x86)\MySQL\Connector NET 8.0\Assemblies\netstandard2.1'
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                description = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                description = ''
                value = 30
            }
        )
    }

    if ($TestConnection) {
        Open-MySqlConnection $ConnectionParams
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-MySqlConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}

$SqlInfoCache = @{}


function Fill-SqlInfoCache {
    param (
        [switch] $Force
    )

    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]600000) {
        return
    }

    # Refresh cache
    $sql_command = New-MySqlCommand "
    SELECT *
    FROM (
        SELECT 
        CONCAT(SC.TABLE_SCHEMA,'.',SC.TABLE_NAME) AS full_object_name,
        'Table' AS object_type,
        SC.COLUMN_NAME,
        (CASE WHEN SC.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END) AS is_primary_key,
        (CASE WHEN SC.EXTRA like '%auto_increment%'  THEN 1 ELSE 0 END) AS is_identity,
        0 AS is_computed,
        (CASE WHEN SC.IS_NULLABLE        = 'N'   THEN 0 ELSE 1 END) AS is_nullable
        FROM       INFORMATION_SCHEMA.SCHEMATA s
        INNER JOIN INFORMATION_SCHEMA.TABLES   st ON  st.TABLE_SCHEMA  = s.SCHEMA_NAME
                                                  AND st.TABLE_CATALOG = s.CATALOG_NAME
        INNER JOIN INFORMATION_SCHEMA.COLUMNS  sc ON  sc.TABLE_SCHEMA  = st.TABLE_SCHEMA
                                                  AND sc.TABLE_NAME    = st.TABLE_NAME
                                                  AND sc.TABLE_CATALOG = st.TABLE_CATALOG
        WHERE sc.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
        UNION
        SELECT 
        CONCAT(SC.TABLE_SCHEMA,'.',SC.TABLE_NAME) AS full_object_name,
        'View' AS object_type,
        SC.COLUMN_NAME,
        (CASE WHEN SC.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END) AS is_primary_key,
        (CASE WHEN SC.EXTRA like '%auto_increment%'  THEN 1 ELSE 0 END) AS is_identity,
        0 AS is_computed,
        (CASE WHEN SC.IS_NULLABLE        = 'N'   THEN 0 ELSE 1 END) AS is_nullable
        FROM       INFORMATION_SCHEMA.SCHEMATA s
        INNER JOIN INFORMATION_SCHEMA.VIEWS   st ON  st.TABLE_SCHEMA  = s.SCHEMA_NAME
                                                  AND st.TABLE_CATALOG = s.CATALOG_NAME
        INNER JOIN INFORMATION_SCHEMA.COLUMNS  sc ON  sc.TABLE_SCHEMA  = st.TABLE_SCHEMA
                                                  AND sc.TABLE_NAME    = st.TABLE_NAME
                                                  AND sc.TABLE_CATALOG = st.TABLE_CATALOG
        WHERE sc.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
    ) a
    ORDER BY full_object_name, COLUMN_NAME
    "

    $result = Invoke-MySqlCommand $sql_command

    Dispose-MySqlCommand $sql_command

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    # Process in one pass
    foreach ($row in $result) {
        if ($row.full_object_name -ne $object.full_name) {
            if ($null -ne $object.full_name) {
                $objects.Add($object) | Out-Null
            }

            $object = @{
                full_name = $row.full_object_name
                type      = $row.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }

        $object.columns.Add(@{
            name           = $row.column_name
            is_primary_key = $row.is_primary_key
            is_identity    = $row.is_identity
            is_computed    = $row.is_computed
            is_nullable    = $row.is_nullable
        }) | Out-Null
    }

    if ($null -ne $object.full_name) {
        $objects.Add($object) | Out-Null
    }

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
}


function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-MySqlConnection $SystemParams

            Fill-SqlInfoCache -Force

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($object in $Global:SqlInfoCache.Objects) {
                    $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                    if ($object.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                        }

                        if ($primary_keys) {
                            # Only supported if primary keys are present
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )

        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #

            Open-MySqlConnection $SystemParams

            Fill-SqlInfoCache

            $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            description = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            description = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Generated' }
                                            if ($_.is_computed)    { 'Computed' }
                                            if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.name })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.name
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #

            Open-MySqlConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                Fill-SqlInfoCache

                $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
                }
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($null -eq $function_params[$key]) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }

            $sql_command = New-MySqlCommand

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { '`' + $_ + '`'  }) -join ', ' }

            switch ($Operation) {
                'Create' {
                    $filter = if ($identity_col) {
                                  "[$identity_col] = SCOPE_IDENTITY()"
                              }
                              elseif ($primary_keys) {
                                  @($primary_keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '
                              }
                              else {
                                  @($function_params.Keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '
                              }

                    $sql_command.CommandText = "
                        INSERT INTO $Class (
                            $(@($function_params.Keys | ForEach-Object { '`' + $_ + '`' }) -join ', ')
                        )
                        VALUES (
                            $(@($function_params.Keys | ForEach-Object { AddParam-MySqlCommand $sql_command $function_params[$_] }) -join ', ')
                        );
                        SELECT TOP(1)
                            $projection
                        FROM
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }

                'Read' {
                    $filter = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }

                    $sql_command.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class$filter
                    "
                    $sql_command.CommandText | Out-File "C:\temp\debug.txt"
                    break
                }

                'Update' {
                    $filter = @($primary_keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        UPDATE TOP(1)
                            $Class
                        SET
                            $(@($function_params.Keys | ForEach-Object { if ($_ -notin $primary_keys) { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" } }) -join ', ')
                        WHERE
                            $filter;
                        SELECT TOP(1)
                            $(@($function_params.Keys | ForEach-Object { '`' + $_ + '`' }) -join ', ')
                        FROM
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }

                'Delete' {
                    $filter = @($primary_keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        DELETE TOP(1)
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }
            }

            if ($sql_command.CommandText) {
                $deparam_command = DeParam-MySqlCommand $sql_command

                LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-MySqlCommand $sql_command $deparam_command
                }
                else {
                    # Log output
                    $rv = Invoke-MySqlCommand $sql_command $deparam_command
                    LogIO info ($deparam_command -split ' ')[0] -Out $rv

                    $rv
                }
            }

            Dispose-MySqlCommand $sql_command

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function New-MySqlCommand {
    param (
        [string] $CommandText
    )

    New-Object MySql.Data.MySqlClient.MySqlCommand($CommandText, $Global:MySqlConnection)
}


function Dispose-MySqlCommand {
    param (
        [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
    )

    $SqlCommand.Dispose()
}


function AddParam-MySqlCommand {
    param (
        [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand,
        $Param
    )

    $param_name = "@param$($SqlCommand.Parameters.Count)_"
    $SqlCommand.Parameters.AddWithValue($param_name, $Param) | Out-Null

    return $param_name
}


function DeParam-MySqlCommand {
    param (
        [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        $value_txt = 
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            else {
                switch ($p.SqlDbType) {
                    { $_ -in @(
                        [System.Data.SqlDbType]::Char
                        [System.Data.SqlDbType]::Date
                        [System.Data.SqlDbType]::DateTime
                        [System.Data.SqlDbType]::DateTime2
                        [System.Data.SqlDbType]::DateTimeOffset
                        [System.Data.SqlDbType]::NChar
                        [System.Data.SqlDbType]::NText
                        [System.Data.SqlDbType]::NVarChar
                        [System.Data.SqlDbType]::Text
                        [System.Data.SqlDbType]::Time
                        [System.Data.SqlDbType]::VarChar
                        [System.Data.SqlDbType]::Xml
                    )} {
                        "'" + $p.Value.ToString().Replace("'", "''") + "'"
                        break
                    }
        
                    default {
                        $p.Value.ToString().Replace("'", "''")
                        break
                    }
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    # Make one single line
    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
}


function Invoke-MySqlCommand {
    param (
        [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand,
        [string] $DeParamCommand
    )

    # Streaming
    # ERAM dbo.Files (426.977 rows) execution time: ?
    function Invoke-MySqlCommand-ExecuteReader {
        param (
            [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            # Read data
            while ($data_reader.Read()) {
                $hash_table = [ordered]@{}

                foreach ($column_name in $column_names) {
                    $hash_table[$column_name] = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                New-Object -TypeName PSObject -Property $hash_table
            }

        }

        $data_reader.Close()
    }

    # Streaming
    # ERAM dbo.Files (426.977 rows) execution time: 16.7 s
    function Invoke-MySqlCommand-ExecuteReader00 {
        param (
            [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            # Initialize result
            $hash_table = [ordered]@{}

            for ($i = 0; $i -lt $column_names.Count; $i++) {
                $hash_table[$column_names[$i]] = ''
            }

            $result = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $result.$column_name = $data_reader[$column_name]
                }

                # Output data
                $result
            }

        }

        $data_reader.Close()
    }

    # Streaming
    # ERAM dbo.Files (426.977 rows) execution time: 01:11.9 s
    function Invoke-MySqlCommand-ExecuteReader01 {
        param (
            [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $field_count = $data_reader.FieldCount

        while ($data_reader.Read()) {
            $hash_table = [ordered]@{}
        
            for ($i = 0; $i -lt $field_count; $i++) {
                $hash_table[$data_reader.GetName($i)] = $data_reader.GetValue($i)
            }

            # Output data
            New-Object -TypeName PSObject -Property $hash_table
        }

        $data_reader.Close()
    }

    # Non-streaming (data stored in $data_table)
    # ERAM dbo.Files (426.977 rows) execution time: 15.5 s
    function Invoke-MySqlCommand-DataAdapter-DataTable {
        param (
            [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
        )

        $data_adapter = New-Object System.Data.SqlClient.SqlDataAdapter($SqlCommand)
        $data_table   = New-Object System.Data.DataTable
        $data_adapter.Fill($data_table) | Out-Null

        # Output data
        $data_table.Rows

        $data_table.Dispose()
        $data_adapter.Dispose()
    }

    # Non-streaming (data stored in $data_set)
    # ERAM dbo.Files (426.977 rows) execution time: 14.8 s
    function Invoke-MySqlCommand-DataAdapter-DataSet {
        param (
            [MySql.Data.MySqlClient.MySqlCommand] $SqlCommand
        )

        $data_adapter = New-Object System.Data.SqlClient.SqlDataAdapter($SqlCommand)
        $data_set     = New-Object System.Data.DataSet
        $data_adapter.Fill($data_set) | Out-Null

        # Output data
        $data_set.Tables[0]

        $data_set.Dispose()
        $data_adapter.Dispose()
    }

    if (! $DeParamCommand) {
        $DeParamCommand = DeParam-MySqlCommand $SqlCommand
    }

    Log debug $DeParamCommand

    try {
        Invoke-MySqlCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log debug "Done"
}


function Open-MySqlConnection {
    param (
        [string] $ConnectionParams
    )
    Log info "OPENING CONNECTION!!!!!"
    $connection_params = ConvertFrom-Json2 $ConnectionParams

    $cs_builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder

    # Use connection related parameters only
    $cs_builder['Server']     = $connection_params.server
    $cs_builder['Database'] = $connection_params.database

    $cs_builder['User ID']  = $connection_params.username
    $cs_builder['Password'] = $connection_params.password   

    if ($connection_params.ssl_mode) {
        $cs_builder['SslMode'] = 'Preferred'
    }

    $connection_string = $cs_builder.ConnectionString
    Log info "DEBUG STRING HERE----------- $($connection_string)"
    if ($Global:MySqlConnection -and $connection_string -ne $Global:MySqlConnectionString) {
        Log info "MySqlConnection connection parameters changed"
        Close-MySqlConnection
    }

    if ($Global:MySqlConnection -and $Global:MySqlConnection.State -ne 'Open') {
        Log warn "MySqlConnection State is '$($Global:MySqlConnection.State)'"
        Close-MySqlConnection
    }

    if ($Global:MySqlConnection) {
        Log debug "Reusing MySqlConnection"
    }
    else {
        Log info "Opening MySqlConnection '$connection_string'"

        try {
            [void][System.Reflection.Assembly]::LoadFrom("$($connection_params.mysql_net_installpath)\MySql.Data.dll")
            $connection = New-Object MySql.Data.MySqlClient.MySqlConnection($connection_string)
            $connection.Open()

            $Global:MySqlConnection       = $connection
            $Global:MySqlConnectionString = $connection_string

            $Global:ColumnsInfoCache = @{}
            $Global:SqlInfoCache = @{}
        }
        catch {
            Log error "Failed: $_"
            Write-Error $_
        }

        Log info "Done"
    }
}


function Close-MySqlConnection {
    if ($Global:MySqlConnection) {
        Log info "Closing MySqlConnection"

        try {
            $Global:MySqlConnection.Close()
            $Global:MySqlConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}
