USE master;
GO
SET NOCOUNT ON;

DECLARE @BackupPath NVARCHAR(4000) = N'/var/opt/mssql/backup/DB_SALES.bak';

IF DB_ID('DB_SALES') IS NULL
BEGIN
  -- Detectar nombres lógicos del .bak
  DECLARE @filelist TABLE
  (
    LogicalName NVARCHAR(128),
    PhysicalName NVARCHAR(260),
    [Type] CHAR(1),
    FileGroupName NVARCHAR(128) NULL,
    [Size] BIGINT,
    MaxSize BIGINT,
    FileId INT,
    CreateLSN NUMERIC(25,0) NULL,
    DropLSN NUMERIC(25,0) NULL,
    UniqueId UNIQUEIDENTIFIER NULL,
    ReadOnlyLSN NUMERIC(25,0) NULL,
    ReadWriteLSN NUMERIC(25,0) NULL,
    BackupSizeInBytes BIGINT NULL,
    SourceBlockSize INT NULL,
    FileGroupId INT NULL,
    LogGroupGUID UNIQUEIDENTIFIER NULL,
    DifferentialBaseLSN NUMERIC(25,0) NULL,
    DifferentialBaseGUID UNIQUEIDENTIFIER NULL,
    IsReadOnly BIT NULL,
    IsPresent BIT NULL,
    TDEThumbprint VARBINARY(32) NULL,
    SnapshotUrl NVARCHAR(360) NULL
  );

  INSERT INTO @filelist
  EXEC('RESTORE FILELISTONLY FROM DISK = ''' + @BackupPath + '''');

  DECLARE @DataLogical NVARCHAR(128) = (SELECT TOP 1 LogicalName FROM @filelist WHERE [Type] = 'D');
  DECLARE @LogLogical  NVARCHAR(128) = (SELECT TOP 1 LogicalName FROM @filelist WHERE [Type] = 'L');

  IF @DataLogical IS NULL OR @LogLogical IS NULL
  BEGIN
    RAISERROR('No se pudieron detectar los nombres logicos del .bak', 16, 1);
    RETURN;
  END

  DECLARE @Sql NVARCHAR(MAX) = N'
  RESTORE DATABASE [DB_SALES]
    FROM DISK = N''' + @BackupPath + N'''
    WITH MOVE N''' + @DataLogical + N''' TO N''/var/opt/mssql/data/DB_SALES.mdf'',
         MOVE N''' + @LogLogical  + N''' TO N''/var/opt/mssql/data/DB_SALES_log.ldf'',
         REPLACE, STATS = 5;';

  PRINT @Sql;
  EXEC(@Sql);
END
ELSE
BEGIN
  PRINT 'DB_SALES ya existe. Omitiendo RESTORE.';
END
GO
