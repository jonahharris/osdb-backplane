#!/usr/local/bin/php -f

This is outside the box
<?

echo "This is inside the box, hello world!\n";
$id = bkpl_connect("test");
bkpl_begin($id);
bkpl_query("SELECT SchemaName, TableName FROM sys.tables");
while (($ary = bkpl_fetch_row()) == true) {
    echo "FETCH $ary[0] $ary[1]\n";
}

bkpl_rollback($id);

?>
This is outside the box

