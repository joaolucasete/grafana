UPDATE {{ .Ident "resource_version" }}
SET
    {{ .Ident "resource_version" }} = {{ .Arg .ResourceVersion }}
WHERE 1 = 1
    AND {{ .Ident "shard" }}    = {{ .Arg .Shard }}
    AND {{ .Ident "group" }}    = {{ .Arg .Group }}
    AND {{ .Ident "resource" }} = {{ .Arg .Resource }}
;
