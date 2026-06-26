#
# MySQL.ps1 - PowerShell system for MySQL (MySqlConnector 2.4.0)
#

$Log_MaskableKeys = @(
    'password'
)

# Load bundled MySqlConnector assembly and dependencies
$script:LibPath = Join-Path (Split-Path $PSScriptRoot -Parent) 'lib' | Join-Path -ChildPath 'MySql'
$script:Dlls = @(
    'System.Runtime.CompilerServices.Unsafe.dll'
    'System.Threading.Tasks.Extensions.dll'
    'System.Buffers.dll'
    'System.Numerics.Vectors.dll'
    'System.Memory.dll'
    'Microsoft.Bcl.AsyncInterfaces.dll'
    'System.Diagnostics.DiagnosticSource.dll'
    'Microsoft.Extensions.DependencyInjection.Abstractions.dll'
    'Microsoft.Extensions.Logging.Abstractions.dll'
    'MySqlConnector.dll'
)

$script:MissingDlls = $script:Dlls | Where-Object { -not (Test-Path (Join-Path $script:LibPath $_)) }
if ($script:MissingDlls) {
    $Global:ModuleStatus = "<b><div class=`"alert alert-danger`" role=`"alert`">MySqlConnector library not installed. Run powershell command below as admin to download and install dependencies automatically.<br /><br /><code>. `"$($PSScriptRoot)\MySQL.ps1`"; Install-MySqlConnector</code><br /></div></b>"
} else {
    if (-not ([System.AppDomain]::CurrentDomain.GetAssemblies() | Where-Object { $_.GetName().Name -eq 'MySqlConnector' })) {
        $resolveHandler = [System.ResolveEventHandler] {
            param($sender, $e)
            $simpleName = ($e.Name -split ',')[0]
            $dllPath = Join-Path $script:LibPath "$simpleName.dll"
            if (Test-Path $dllPath) {
                return [System.Reflection.Assembly]::LoadFrom($dllPath)
            }
            return $null
        }
        [System.AppDomain]::CurrentDomain.add_AssemblyResolve($resolveHandler)

        foreach ($dll in $script:Dlls) {
            [System.Reflection.Assembly]::LoadFrom((Join-Path $script:LibPath $dll)) | Out-Null
        }
    }
    $Global:ModuleStatus = "<b><div class=`"alert alert-success`" role=`"alert`">Using MySqlConnector 2.4.0</div></b>"
}

function Install-MySqlConnector {
    <#
    .SYNOPSIS
        Downloads and installs MySqlConnector 2.4.0 dependencies.
    .DESCRIPTION
        Downloads NuGet packages from nuget.org, extracts netstandard2.0 DLLs,
        and places them in the lib\MySqlConnector folder.
    .PARAMETER Force
        Overwrite existing DLLs.
    #>
    param([switch]$Force)

    $Packages = @(
        @{ Name = 'System.Runtime.CompilerServices.Unsafe'; Version = '4.5.3' }
        @{ Name = 'System.Threading.Tasks.Extensions';    Version = '4.5.4' }
        @{ Name = 'System.Buffers';                       Version = '4.4.0' }
        @{ Name = 'System.Numerics.Vectors';               Version = '4.5.0' }
        @{ Name = 'System.Memory';                         Version = '4.5.5' }
        @{ Name = 'Microsoft.Bcl.AsyncInterfaces';        Version = '8.0.0' }
        @{ Name = 'System.Diagnostics.DiagnosticSource';   Version = '8.0.1' }
        @{ Name = 'Microsoft.Extensions.DependencyInjection.Abstractions'; Version = '8.0.2' }
        @{ Name = 'Microsoft.Extensions.Logging.Abstractions'; Version = '8.0.2' }
        @{ Name = 'MySqlConnector';                        Version = '2.4.0' }
    )

    $target = $script:LibPath
    $null = New-Item -ItemType Directory -Path $target -Force
    $count = 0
    $total = $Packages.Count

    foreach ($pkg in $Packages) {
        $count++
        $dllName = "$($pkg.Name).dll"
        $outPath = Join-Path $target $dllName

        if ((Test-Path $outPath) -and -not $Force) {
            Write-Host "[$count/$total] $($pkg.Name) $($pkg.Version) - exists, skip (use -Force to overwrite)"
            continue
        }

        Write-Host "[$count/$total] $($pkg.Name) $($pkg.Version) ... " -NoNewline
        $nupkgUrl = "https://www.nuget.org/api/v2/package/$($pkg.Name)/$($pkg.Version)"
        $tmpZip = Join-Path $env:TEMP "$($pkg.Name).$($pkg.Version).zip"

        try { Invoke-WebRequest -Uri $nupkgUrl -OutFile $tmpZip -UseBasicParsing -ErrorAction Stop }
        catch { Write-Host "FAILED (download)"; continue }

        $extractDir = Join-Path $env:TEMP "$($pkg.Name).$($pkg.Version).ext"
        $null = New-Item -ItemType Directory -Path $extractDir -Force

        try { Expand-Archive -Path $tmpZip -DestinationPath $extractDir -Force -ErrorAction Stop }
        catch { Write-Host "FAILED (extract): $($_.Exception.Message)"; Remove-Item $tmpZip,$extractDir -Recurse -Force -ErrorAction SilentlyContinue; continue }

        $dllSource = Get-ChildItem -Path $extractDir -Recurse -Filter $dllName | Where-Object { $_.DirectoryName -match '\\netstandard2\.0$' } | Select-Object -First 1
        if (-not $dllSource) {
            $dllSource = Get-ChildItem -Path $extractDir -Recurse -Filter $dllName | Where-Object { $_.DirectoryName -match '\\lib\\' } | Select-Object -First 1
        }

        if (-not $dllSource) { Write-Host "FAILED (DLL not found)" }
        else { Copy-Item -Path $dllSource.FullName -Destination $outPath -Force; Write-Host "OK" }

        Remove-Item $tmpZip,$extractDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

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

    Log verbose "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'ModuleStatus'
                type = 'text'
                label = 'Driver Status'
                text = $Global:ModuleStatus
            }    
           @{
                name = 'server'
                type = 'textbox'
                label = 'Server'
                description = 'Hostname/IP of server'
                value = ''
            }
            @{
                name = 'port'
                type = 'textbox'
                label = 'Port'
                description = 'Port of Server'
                value = '3306'
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
                description = 'Name of database'
                value = ''
            }
            @{
                name = 'username'
                type = 'textbox'
                label = 'Username'
                description = 'User account name to access server'
                value = ''
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                description = 'User account password to access server'
                value = ''
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

    Log verbose "Done"
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
    $sql_command = New-MySqlCommand @'
        SELECT *
        FROM (
            SELECT 
                CONCAT(sc.TABLE_SCHEMA, '.', sc.TABLE_NAME) AS full_object_name,
                'Table' AS object_type,
                sc.COLUMN_NAME,
                (CASE WHEN sc.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END) AS is_primary_key,
                (CASE WHEN sc.EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END) AS is_identity,
                0 AS is_computed,
                (CASE WHEN sc.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END) AS is_nullable
            FROM INFORMATION_SCHEMA.TABLES st
            INNER JOIN INFORMATION_SCHEMA.COLUMNS sc
                ON sc.TABLE_SCHEMA = st.TABLE_SCHEMA
            AND sc.TABLE_NAME   = st.TABLE_NAME
            WHERE st.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
            AND st.TABLE_TYPE = 'BASE TABLE'

            UNION ALL

            SELECT 
                CONCAT(sc.TABLE_SCHEMA, '.', sc.TABLE_NAME) AS full_object_name,
                'View' AS object_type,
                sc.COLUMN_NAME,
                0 AS is_primary_key,
                0 AS is_identity,
                0 AS is_computed,
                (CASE WHEN sc.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END) AS is_nullable
            FROM INFORMATION_SCHEMA.VIEWS v
            INNER JOIN INFORMATION_SCHEMA.COLUMNS sc
                ON sc.TABLE_SCHEMA = v.TABLE_SCHEMA
            AND sc.TABLE_NAME   = v.TABLE_NAME
            WHERE v.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
        ) a
        ORDER BY full_object_name, COLUMN_NAME
'@

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

    Log verbose "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"

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
                                    '`' + $identity_col + '`' + ' = LAST_INSERT_ID()'
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
                        SELECT
                            $projection
                        FROM
                            $Class
                        WHERE
                            $filter
                        LIMIT 1
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
                    break
                }

                'Update' {
                    $filter = @($primary_keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        UPDATE
                            $Class
                        SET
                            $(@($function_params.Keys | ForEach-Object { if ($_ -notin $primary_keys) { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" } }) -join ', ')
                        WHERE
                            $filter
                        LIMIT 1 ;
                        SELECT
                            $(@($function_params.Keys | ForEach-Object { '`' + $_ + '`' }) -join ', ')
                        FROM
                            $Class
                        WHERE
                            $filter
                        LIMIT 1
                    "
                    break
                }

                'Delete' {
                    $filter = @($primary_keys | ForEach-Object { '`' + $_ + '`' + " = $(AddParam-MySqlCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        DELETE FROM
                            $Class
                        WHERE
                            $filter
                        LIMIT 1
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

                    log info ($rv | ConvertTo-Json)
                    $rv
                }
            }

            Dispose-MySqlCommand $sql_command

        }

    }

    Log verbose "Done"
}


#
# Helper functions
#

function New-MySqlCommand {
    param (
        [string] $CommandText
    )

    $cmd = $Global:MySqlConnection.CreateCommand()
    $cmd.CommandText = $CommandText
    $cmd
}


function Dispose-MySqlCommand {
    param (
        $SqlCommand
    )

    $SqlCommand.Dispose()
}


function AddParam-MySqlCommand {
    param (
        $SqlCommand,
        $Param
    )

    $param_name = "@param$($SqlCommand.Parameters.Count)_"
    $p = $SqlCommand.CreateParameter()
    $p.ParameterName = $param_name
    $p.Value = if ($null -eq $Param) { [System.DBNull]::Value } else { $Param }
    $SqlCommand.Parameters.Add($p) | Out-Null

    return $param_name
}


function DeParam-MySqlCommand {
    param (
        $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        $value_txt =
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            elseif ($p.Value -is [DateTime]) {
                "'$($p.Value.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            elseif ($p.Value -is [TimeSpan]) {
                $ts = $p.Value
                $totalHours = [Math]::Floor($ts.TotalHours)
                "'$('{0}:{1:D2}:{2:D2}' -f $totalHours, $ts.Minutes, $ts.Seconds)'"
            }
            elseif ($p.Value -is [string] -and $p.Value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                $parsed = [DateTime]::Parse($p.Value, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
                "'$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            elseif ($p.Value -is [string] -and $p.Value -match '^\d{13}$') {
                $parsed = [DateTimeOffset]::FromUnixTimeMilliseconds([long]$p.Value).UtcDateTime
                "'$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            else {
                $val = $p.Value.ToString()
                if ($p.Value -is [string]) {
                    "'$($val.Replace("'", "''"))'"
                } else {
                    $val.Replace("'", "''")
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
}


function Invoke-MySqlCommand {
    param (
        $SqlCommand,
        [string] $DeParamCommand
    )

    # Streaming
    function Invoke-MySqlCommand-ExecuteReader {
        param (
            $SqlCommand
        )
        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {
            while ($data_reader.Read()) {
                $hash_table = [ordered]@{}

                foreach ($column_name in $column_names) {
                    
                    $value_txt = if ($data_reader[$column_name] -eq [System.DBNull]::Value) {
                                        $null
                                    }
                                    elseif ($data_reader[$column_name] -is [DateTime]) {
                                        "$($data_reader[$column_name].ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    elseif ($data_reader[$column_name] -is [TimeSpan]) {
                                        $ts = $data_reader[$column_name]
                                        $totalHours = [Math]::Floor($ts.TotalHours)
                                        "$('{0}:{1:D2}:{2:D2}' -f $totalHours, $ts.Minutes, $ts.Seconds)"
                                    }
                                    elseif ($data_reader[$column_name] -is [string] -and $data_reader[$column_name] -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                                        $parsed = [DateTime]::Parse($data_reader[$column_name], [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                        "$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    elseif ($data_reader[$column_name] -is [string] -and $data_reader[$column_name] -match '^\d{13}$') {
                                        $parsed = [DateTimeOffset]::FromUnixTimeMilliseconds([long]$data_reader[$column_name]).UtcDateTime
                                        "$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    else {
                                        $val = $data_reader[$column_name].ToString()
                                        if ($data_reader[$column_name] -is [string]) {
                                            "$($data_reader[$column_name])"
                                        } else {
                                            $data_reader[$column_name]
                                        }
                                    }
                    
                    #$hash_table[$column_name] = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                    $hash_table[$column_name] = $value_txt
                }

                New-Object -TypeName PSObject -Property $hash_table
            }

        }

        $data_reader.Close()
    }

    if (! $DeParamCommand) {
        $DeParamCommand = DeParam-MySqlCommand $SqlCommand
        
    }
    
    Log debug $DeParamCommand
    $SqlCommand.CommandText = $DeparamCommand

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
    $connection_params = ConvertFrom-Json2 $ConnectionParams

    # Build connection string using MySqlConnectionStringBuilder (SimplySQL pattern)
    $sb = New-Object MySqlConnector.MySqlConnectionStringBuilder
    $sb.Server = $connection_params.server
    $sb.Port = [int]$connection_params.port
    $sb.Database = $connection_params.database
    $sb.UserID = $connection_params.username
    $sb.Password = $connection_params.password
    $sb.AllowLoadLocalInfile = $true
    $sb.UseAffectedRows = $true
    $sb.AllowUserVariables = $true
    $sb.TlsVersion = 'Tls12'

    if ($connection_params.ssl_mode) {
        $sb.SslMode = [MySqlConnector.MySqlSslMode]::Preferred
    }
    $connection_string = $sb.ToString()

    if ($Global:MySqlConnection -and $connection_string -ne $Global:MySqlConnectionString) {
        Log verbose "MySqlConnection connection parameters changed"
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
        Log verbose "Opening MySqlConnection '$connection_string'"

        try {
            $connection = New-Object MySqlConnector.MySqlConnection($connection_string)
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

        Log verbose "Done"
    }
}


function Close-MySqlConnection {
    if ($Global:MySqlConnection) {
        Log verbose "Closing MySqlConnection"

        try {
            $Global:MySqlConnection.Close()
            $Global:MySqlConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log verbose "Done"
    }
}
