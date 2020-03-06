package ru.eludia.base.db.dialect;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import ru.eludia.base.DB;
import ru.eludia.base.db.sql.build.QP;
import ru.eludia.base.db.sql.build.TableSQLBuilder;
import ru.eludia.base.model.abs.AbstractCol;
import ru.eludia.base.model.Col;
import ru.eludia.base.Model;
import ru.eludia.base.model.Key;
import ru.eludia.base.model.phys.PhysicalCol;
import ru.eludia.base.model.phys.PhysicalKey;
import ru.eludia.base.model.phys.PhysicalKeyPart;
import ru.eludia.base.model.phys.PhysicalTable;
import ru.eludia.base.model.Ref;
import ru.eludia.base.model.Table;
import ru.eludia.base.model.Trigger;
import ru.eludia.base.model.Type;
import ru.eludia.base.model.View;
import ru.eludia.base.model.def.Const;
import ru.eludia.base.model.def.Def;
import ru.eludia.base.model.diff.TypeAction;
import ru.eludia.base.model.phys.PhysicalModel;
import ru.eludia.base.db.sql.build.SQLBuilder;
import ru.eludia.base.model.Procedure;
import static ru.eludia.base.model.def.Blob.EMPTY_BLOB;
import ru.eludia.base.model.phys.PhysicalView;

public final class Oracle extends ANSI {
    
    private static MessageDigest md5;
    
    static {
        try {
            md5 = MessageDigest.getInstance ("MD5");
        }
        catch (NoSuchAlgorithmException ex) {
            Logger.getLogger (Oracle.class.getName()).log (Level.SEVERE, null, ex);
        }
    }
    
    private static final int LEN_INTEGER = 10;
    private static final int LEN_VARCHAR = 4000;
    private static final int LEN_MONEY = 15;
    private static final int PRC_MONEY = 2;
    
    public Oracle (Connection cn, Model model) {
        super (cn, model);
    }
    
    @Override
    public final Object getValue (ResultSet rs, int n) throws SQLException {
        
        ResultSetMetaData md = rs.getMetaData ();
                
        JDBCType t = JDBCType.valueOf (md.getColumnType (n));

        switch (t) {
            case DATE:
            case TIMESTAMP:
                Timestamp ts = rs.getTimestamp (n);
                return rs.wasNull () ? null : ts.toString ();
            case NUMERIC:
                BigDecimal bd = rs.getBigDecimal (n);
                if (rs.wasNull ()) return null;
                if (md.getScale (n) > 0) return bd;
                if (bd.scale () > 0) return bd;
                if (bd.precision () > 18) return bd;
                return bd.longValue ();
            case CLOB:
            case VARCHAR:
	    case CHAR:
                String s = rs.getString (n);
                return rs.wasNull () ? "" : s;
            case VARBINARY:
                byte [] b = rs.getBytes (n);
                return 
                    rs.wasNull ()  ? null : 
                    b.length == 16 ? to.UUID (b) : 
                    to.hex (b);
            default:
                throw new IllegalArgumentException ("Not supported: " + t);
        }
        
    }

    @Override
    protected String toVarbinary (Object v) {
        return v.toString ().toUpperCase ().replace ("-", "");
    }
    
    @Override
    protected Col toCanonical (Col col) {

        switch (col.getType ()) {
            case BOOLEAN:
                return new Col (col.getName (), Type.NUMERIC, 1, col.getRemark ());
            case INTEGER:
                return new Col (col.getName (), Type.NUMERIC, (col.getLength (LEN_INTEGER)), 0, col.getRemark ());
            case MONEY:
                return new Col (col.getName (), Type.NUMERIC, (col.getLength (LEN_MONEY)), (col.getPrecision (PRC_MONEY)), col.getRemark ());
            case STRING:
                return col.getLength () <= LEN_VARCHAR ? 
                    new Col (col.getName (), Type.STRING, (col.getLength (LEN_VARCHAR)), col.getRemark ()) : 
                    new Col (col.getName (), Type.TEXT, col.getRemark ());
            default:
                return col;
        }

    }

    @Override
    protected PhysicalCol toBasicPhysical (Col col) {
        
        switch (col.getType ()) {
            case BLOB:
                return new PhysicalCol (JDBCType.BLOB, col.getName ());
            case TEXT:
                return new PhysicalCol (JDBCType.CLOB, col.getName ());
            case DATE:
            case DATETIME:
                return new PhysicalCol (JDBCType.DATE, col.getName ());                
            case NUMERIC:
                return new PhysicalCol (JDBCType.NUMERIC, col.getName (), col.getLength (), col.getPrecision ());                
            case STRING:
                return new PhysicalCol (JDBCType.VARCHAR, col.getName (), col.getLength ());
            case TIMESTAMP:
                return new PhysicalCol (JDBCType.TIMESTAMP, col.getName ());                
            case UUID:
                return new PhysicalCol (JDBCType.VARBINARY, col.getName (), 16);
            case BINARY:
                return new PhysicalCol (JDBCType.VARBINARY, col.getName (), col.getLength ());
            default:
                throw new IllegalArgumentException ("Not supported: " + col);
        }

    }
    
    protected String getTypeName (PhysicalCol col) {
        
        if (col == null) throw new IllegalStateException ("PhysicalCol col==null: probably, adjustTable is not called");

        switch (col.getType ()) {
            case BLOB:
                return "BLOB";
            case CLOB:
                return "CLOB";
            case DATE:
                return "DATE";
            case NUMERIC:
                return "NUMBER";
            case VARCHAR:
                return "VARCHAR2";
            case TIMESTAMP:
                return "TIMESTAMP";
            case VARBINARY:
                return "RAW";
            default:
                throw new IllegalArgumentException ("Not supported: " + col);
        }

    }
    
    @Override
    protected final void comment (Table table) throws SQLException {
        
        d0 ("COMMENT ON TABLE " + table.getName () + " IS '" + table.getRemark () + "'");
        
    }

    @Override
    protected final void comment (Table table, AbstractCol col) throws SQLException {

        d0 ("COMMENT ON COLUMN " + table.getName () + '.' + col.getName () + " IS '" + col.getRemark ().replaceAll ("'", "''") + "'");

    }

    @Override
    protected final QP genCreateSql (Table table) throws SQLException {
                
        QP qp = new QP ("CREATE ");
        
        if (table.getTemporalityType () != null) {
            qp.append (table.getTemporalityType ().toString ());
            qp.append (" TEMPORARY ");
        }
        
        qp.append ("TABLE ");
        qp.append (table.getName ());
        qp.append (" (");
                
        table.getColumns ().forEach ((name, col) -> {
            qp.append (name);
            qp.append (' ');
            appendColDefinition (qp, col.toPhysical (), true);
            qp.append (',');
        });
        
        qp.setLastChar (')');
        
        if (table.getTemporalityRowsAction () != null) {
            qp.append (" ON COMMIT ");
            qp.append (table.getTemporalityRowsAction ().toString ());
            qp.append (" ROWS");
        }

        return qp;

    }    

    @Override
    protected final QP genCreateSql (Table table, PhysicalCol col) throws SQLException {
                
        QP qp = new QP ("ALTER TABLE ");
        qp.append (table.getName ());
        qp.append (" ADD ");
        qp.append (col.getName ());
        qp.append (' ');
        appendColDefinition (qp, col, true);
        
        return qp;
        
    }    
    
    @Override
    protected final QP genCreateSql (Ref ref) throws SQLException {
        
        if (ref.getTable ().getTemporalityType () != null) return null;
        if (ref.getTargetTable () instanceof View) return null;
                
        QP qp = new QP ("ALTER TABLE ");
        qp.append (ref.getTable ().getName ());
        qp.append (" ADD FOREIGN KEY (");
        qp.append (ref.getName ());
        qp.append (") REFERENCES ");
        qp.append (ref.getTargetTable ().getName ());
        
        return qp;
        
    }    
    
    @Override    
    protected final QP genDropRefSql (PhysicalTable t, PhysicalCol c) throws SQLException {
                        
        QP qp = new QP ("ALTER TABLE ");
        qp.append (t.getName ());
        qp.append (" DROP CONSTRAINT ");
        qp.append (c.getFk ());
        
        return qp;
        
    }    

    @Override
    public TypeAction getTypeAction (JDBCType asIs, JDBCType toBe) {
                
        if (asIs == toBe) return null;
            
        if (asIs == JDBCType.TIMESTAMP && toBe == JDBCType.DATE) return null;
        if (asIs == JDBCType.CLOB      && toBe == JDBCType.VARCHAR) return null;

        return TypeAction.RECREATE;
            
    }

    @Override
    protected QP genAlterSql (Table table, PhysicalCol col) throws SQLException {
        
        logger.info ("Altering " + table.getName () + '.' + col.getName ());
        
        QP qp = new QP ("ALTER TABLE ");
        qp.append (table.getName ());
        qp.append (" MODIFY (");
        qp.append (col.getName ());
        qp.append (' ');
        appendColDefinition (qp, col, false);
        qp.append (')');
        
        return qp;
        
    }
        
    @Override
    protected QP genNullableSql (Table table, PhysicalCol col, String nn) throws SQLException {

        logger.info ("Setting nullability for " + table.getName () + '.' + col.getName ());
        
        QP qp = new QP ("ALTER TABLE ");
        qp.append (table.getName ());
        qp.append (" MODIFY (");
        qp.append (col.getName ());
        qp.append (' ');
        qp.append (nn);
        qp.append (')');
        
        return qp;

    }

    @Override
    protected void recreate (Table table, PhysicalCol col) throws SQLException {
        
        if (col.isVirtual ()) {            
            drop (table, col);
            create (table, col);            
        }
        else {
            recreateWithDataMigration (col, table);
        }

    }

    private void recreateWithDataMigration (PhysicalCol col, Table table) throws SQLException {
        
        PhysicalCol tmp = new PhysicalCol (col.getType (), "tmp_" + System.currentTimeMillis (), col.getLength (), col.getPrecision ());
        tmp.setDef (col.getDef ());
        tmp.setNullable (col.isNullable ());
        
        create (table, tmp);
        
        d0 ("UPDATE " + table.getName () + " SET " + tmp.getName () + " = " + col.getName ());
        drop (table, col);
        d0 ("ALTER TABLE " + table.getName () + " RENAME COLUMN " + tmp.getName () + " TO " + col.getName ());
        
    }

    private void drop (Table table, PhysicalCol col) throws SQLException {
        d0 ("ALTER TABLE " + table.getName () + " DROP COLUMN " + col.getName ());
    }

    @Override
    protected String toSQL (Def def, JDBCType type) {
        
        if (EMPTY_BLOB.equals (def)) return "EMPTY_BLOB()";
        
        if (def instanceof ru.eludia.base.model.def.UUID) return "SYS_GUID()";
        
        if (def instanceof ru.eludia.base.model.def.Now) {
            
            switch (type) {
                case DATE:
                    return "SYSDATE";
                case TIMESTAMP:
                    return "CURRENT_TIMESTAMP";
                default:
                    throw new IllegalArgumentException ("NOW is not supported for JDBC type #" + type);                    
            }
            
        };
        
        if (def instanceof Const) return ((Const) def).toSql ();
        
        throw new IllegalArgumentException ("Not supported default value: " + def);
        
    }
    
    private boolean isInParens (String s) {
        return 
            s.charAt               (0) == '(' && 
            s.charAt (s.length () - 1) == ')';
    }
    
    private String stripQuotes (String s) {

        if (s.indexOf ('"') < 0 && s.indexOf (' ') < 0) return s;

        StringBuilder sb = new StringBuilder ();

        boolean inApos = false;

        for (int i = 0; i < s.length (); i ++) {

            char c = s.charAt (i);

            if (c == '\'') inApos = !inApos;

            if (!inApos) switch (c) {
                case '"': continue;
                case ' ': continue;
            }
            
            sb.append (c);
            
        }
        
        return sb.toString ();
        
    }
    
    private boolean eqVirtDef (String a, String b) {
        
        if (isInParens (b)) b = b.substring (1, b.length () - 1);

        a = stripQuotes (a);
        b = stripQuotes (b);

        if (a.equals (b)) return true;

        if (a.startsWith ("TO_CHAR(") && a.equals ("TO_CHAR(" + b + ')')) return true;

        return false;
        
    }

    @Override
    public boolean equalDef (PhysicalCol asIs, PhysicalCol toBe) {
        
        if (super.equalDef (asIs, toBe)) return true;
        
        String a = asIs.getDef ();
        String b = toBe.getDef ();
        
        if (a == null || b == null) return false; // both nulls are handled by super method

        int la = a.length ();
        int lb = b.length ();
        int dl = lb - la;
        
        if (dl == -1 && a.endsWith (" ") && a.startsWith (b)) return true; // somtimes, Oracle appends spaces
        
        if (toBe.isVirtual ()) return eqVirtDef (a, b);
            
        return false;
        
    }        

    @Override
    protected PhysicalModel getExistingModel () throws SQLException {
        
        PhysicalModel m = new PhysicalModel ();
        
        forEach (new QP ("SELECT table_name FROM user_tables"), rs -> {m.add (new PhysicalTable (rs));});
        forEach (new QP ("SELECT view_name table_name, text FROM user_views"), rs -> {m.add (new PhysicalView (rs));});

        forEach (new QP ("SELECT * FROM user_tab_comments WHERE table_type IN ('TABLE', 'VIEW')"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
                            
            t.setRemark (rs.getString ("COMMENTS"));
            
        });
        
        Map <String, String> pks = new HashMap <> ();
        
        forEach (new QP ("SELECT TABLE_NAME, COLUMN_NAME FROM user_cons_columns WHERE position = 1 AND constraint_name IN (SELECT constraint_name FROM user_constraints WHERE constraint_type = 'P')"), rs -> {
            
            pks.put (rs.getString ("TABLE_NAME"), rs.getString ("COLUMN_NAME"));
            
        });
                
        DatabaseMetaData md = cn.getMetaData ();
        
        try (java.sql.ResultSet rs = md.getColumns (cn.getCatalog (), cn.getSchema (), null, null)) {

            while (rs.next ()) {

                PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));

                if (t == null) continue;

                String pk = pks.get (t.getName ().toUpperCase ());

                PhysicalCol col = new PhysicalCol (rs);

                if (pk != null && col.getName ().toUpperCase ().equals (pk.toUpperCase ())) t.pk (col); else t.add (col);

            }
            
        }
        
        forEach (new QP ("SELECT a.constraint_name fk, a.table_name, a.column_name, c_pk.table_name ref FROM user_cons_columns a INNER JOIN user_constraints c ON a.constraint_name = c.constraint_name INNER JOIN user_constraints c_pk ON c.r_constraint_name = c_pk.constraint_name WHERE c.constraint_type = 'R'"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
            
            PhysicalCol c = t.getColumn (rs.getString ("COLUMN_NAME"));
            
            if (c == null) return;
            
            c.setRef (rs.getString ("REF"));
            c.setFk  (rs.getString ("FK"));

        });

        forEach (new QP ("SELECT * FROM user_col_comments"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
            
            PhysicalCol col = (PhysicalCol) t.getColumns ().get (rs.getString ("COLUMN_NAME"));
                
            col.setRemark (rs.getString ("COMMENTS"));
            
        });
        
        forEach (new QP ("SELECT trigger_name, trigger_type, triggering_event, table_name, trigger_body FROM user_triggers"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
            
            String type = rs.getString ("TRIGGER_TYPE");
            
            if (!type.endsWith (" EACH ROW")) return;

            final String logMsg = "PhysicalModel.getExistingModel table_name=" + rs.getString ("TABLE_NAME") + ", trigger_name=" + rs.getString ("TRIGGER_NAME") + ", trigger_type=" + type + ", triggering_event=" + rs.getString ("TRIGGERING_EVENT");

            logger.log (Level.INFO, logMsg);

            t.getTriggers ().add (new Trigger (type.substring (0, type.length () - "EACH ROW".length ()) + rs.getString ("TRIGGERING_EVENT"), rs.getString ("TRIGGER_BODY")));

            logger.log (Level.INFO, logMsg + " done.");

        });        

        return m;
                      
    }

    @Override
    protected void addIndexes (PhysicalModel m) throws SQLException {
        
        forEach (new QP ("SELECT * FROM user_indexes WHERE index_type LIKE '%NORMAL'"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
            
            PhysicalKey key = new PhysicalKey (rs.getString ("INDEX_NAME"));
            
            key.setUnique ("UNIQUE".equals (rs.getString ("UNIQUENESS")));
            
            t.add (key);
            
        });
        
        forEach (new QP ("SELECT table_name, index_name, column_position, column_name, descend FROM user_ind_columns ORDER BY 1, 2, 3"), rs -> {
            
            PhysicalTable t = m.get (rs.getString ("TABLE_NAME"));
            
            if (t == null) return;
            
            PhysicalKey k = new PhysicalKey (rs.getString ("INDEX_NAME"));
            
            PhysicalKey key = (PhysicalKey) t.getKeys ().get (k.getName ());

            if (key == null) return;

            PhysicalKeyPart part = new PhysicalKeyPart ((PhysicalCol) t.getColumns ().get (rs.getString ("COLUMN_NAME")));
            
            if ("DESC".equals (rs.getString ("DESCEND"))) part.setIsDesc (true);
            
            key.getParts ().add (part);
            
        });
        
    }

    @Override
    protected void genUpsertSql (TableSQLBuilder b) {
        b.append ("MERGE INTO ");

        b.append (b.getTable ().getName ());        
        b.append (" \"__old\" USING (SELECT ");
        for (PhysicalCol col: b.getCols ()) {
            b.append ("? ");
            b.append (col.getName ());
            b.append (',');
        }
        b.setLastChar (' ');
        b.append ("FROM DUAL) \"__new\" ON (");
        
        for (PhysicalCol col: b.getKeyCols ()) {            
            if (b.getLastChar () != '(') b.append (" AND");
            b.append ("\"__old\".");
            b.append (col.getName ());
            b.append ("=\"__new\".");
            b.append (col.getName ());
        }
        
        b.append (") ");

        if (!b.getNonKeyCols ().isEmpty ()) {

            b.append ("WHEN MATCHED THEN UPDATE SET ");
            for (PhysicalCol col: b.getNonKeyCols ()) {
                if (col.isVirtual ()) continue;
                String name = col.getName ();
                b.append (name);
                b.append ("=\"__new\".");
                b.append (name);
                b.append (',');
            }
            b.setLastChar (' ');

        }
        
        b.append ("WHEN NOT MATCHED THEN INSERT (");
        for (PhysicalCol col: b.getCols ()) {
            if (col.isVirtual ()) continue;
            b.append (col.getName ());
            b.append (',');
        }
        b.setLastChar (')');
        b.append (" VALUES (");
        for (PhysicalCol col: b.getCols ()) {
            if (col.isVirtual ()) continue;
            b.append ("\"__new\".");
            b.append (col.getName ());
            b.append (',');
        }
        b.setLastChar (')');

    }

    @Override
    protected PhysicalKey toPhysical (Table table, Key k) {
        
        String name = "ix_" + table.getName () + '_' + k.getName ();
        
        if (name.length () > 30) name = "ix_" + to.hex (md5.digest (name.getBytes ())).substring (0, 27);
        
        PhysicalKey result = new PhysicalKey (name.toUpperCase ());
        
        result.setUnique (k.isUnique ());
        
        for (String s: k.getParts ()) {
            
            StringTokenizer st = new StringTokenizer (s, " ");
            
            String colName = st.nextToken ();
            
            final Col column = table.getColumn (colName);
            
            if (column == null) throw new IllegalArgumentException ("Column not found: " + table.getName () + "." + colName);
            
            PhysicalCol col = column.toPhysical ();
            
            PhysicalKeyPart part = new PhysicalKeyPart (col);
            
            part.setIsDesc (s.toUpperCase ().endsWith (" DESC"));
            
            result.getParts ().add (part);
            
        }

        return result;

    }

    @Override
    protected QP genCreateSql (Table table, PhysicalKey key) throws SQLException {

        QP qp = new QP ("CREATE ");
        if (key.isUnique ()) qp.append ("UNIQUE ");
        qp.append ("INDEX ");
        qp.append (key.getName ());
        qp.append (" ON ");
        qp.append (table.getName ());
        qp.append ("(");
        for (PhysicalKeyPart i: key.getParts ()) {
            qp.append (i.getCol ().getName ());
            if (i.isDesc ()) qp.append (" DESC");
            qp.append (',');
        }
        qp.setLastChar (')');
        
        return qp;

    }
    
    @Override
    protected QP genDropSql (Table table, PhysicalKey key) throws SQLException {
        
        QP qp = new QP ("DROP INDEX ");
        qp.append (key.getName ());
        return qp;

    }

    @Override
    protected void update (View view)  throws SQLException {
        d0 ("CREATE OR REPLACE FORCE VIEW " + view.getName () + " AS " + view.getSQL ());
    }

    @Override
    protected void appendColDefaultExpression (QP qp, PhysicalCol col) {
        
        if (col.isVirtual ()) {
            qp.append (" AS (");
            qp.append (col.getDef ());
            qp.append (")");
        }
        else {
            super.appendColDefaultExpression (qp, col);
        }

    }

    @Override
    protected void update (Table table, Trigger trg) throws SQLException {

        String name = "ix_" + table.getName () + '_' + trg.getName ();

        if (name.length () > 30) name = "tr_" + to.hex (md5.digest (name.getBytes ())).substring (0, 27);

        d0 ("CREATE OR REPLACE TRIGGER " + name + " " + trg.getWhen () + " ON " + table.getName () + " FOR EACH ROW " + trg.getWhat ());

    }
    
    @Override
    protected void update (Procedure p) throws SQLException {
        
        StringBuilder sb = new StringBuilder ("CREATE OR REPLACE PROCEDURE ");
        
        sb.append (p.getName ());
        
        if (ok (p.getParams ())) {
            sb.append (" (");
            sb.append (p.getParams ());
            sb.append (')');
        }
        
        sb.append (" AS ");
        sb.append (p.getBody ());
        
        d0 (sb.toString ());

    }
    
    private static final PhysicalCol dummyIntCol = new PhysicalCol (JDBCType.INTEGER, "");

    @Override
    public QP toLimitedQP (QP inqp, int offset, Integer limit) {
        
        QP qp = new QP ("SELECT * FROM(SELECT rownum rnum, a.* FROM(");        
        qp.add (inqp);        
        qp.add (") a WHERE rownum <= ?", (offset + limit), dummyIntCol);
        qp.add (") WHERE rnum > ?", offset, dummyIntCol);
        
        return qp;
        
    }

    @Override
    protected SQLBuilder genUpsertSql (Table t, Table records, String [] key) {
        
        QP b = new QP ("MERGE INTO "); 
        
        b.append (t.getName ());        
        b.append (" \"__old\" USING (SELECT * FROM ");
        b.append (records.getName ());
        b.append (") \"__new\" ON (");
        
        for (String col: key) {            
            if (b.getLastChar () != '(') b.append (" AND");
            b.append ("\"__old\".");
            b.append (col);
            b.append ("=\"__new\".");
            b.append (col);
        }
        
        b.append (") ");
        
        List<String> keyCols = Arrays.asList (key);
        Collection<Col> cols = records.getColumns ().values ();
        
        if (t.getColumns ().size () > keyCols.size ()) {

            b.append ("WHEN MATCHED THEN UPDATE SET ");
            for (Col col: cols) {
                String name = col.getName ();
                if (keyCols.contains (name)) continue;
                b.append (name);
                b.append ("=\"__new\".");
                b.append (name);
                b.append (',');
            }
            b.setLastChar (' ');

        }
        
        b.append ("WHEN NOT MATCHED THEN INSERT (");
        for (Col col: cols) {
            b.append (col.getName ());
            b.append (',');
        }
        b.setLastChar (')');
        b.append (" VALUES (");
        for (Col col: cols) {
            b.append ("\"__new\".");
            b.append (col.getName ());
            b.append (',');
        }
        b.setLastChar (')');
        
        return b;

    }

    @Override
    protected SQLBuilder genTruncateSql (Table t) {
        QP qp = new QP ("TRUNCATE TABLE ");
        qp.append (t.getName ());
        return qp;
    }
    
    @Override
    protected QP genSetPkSql (Table table) {
        QP qp = new QP ("ALTER TABLE ");
        qp.append (table.getName ());
        qp.append (" ADD PRIMARY KEY (");
        for (Col i: table.getPk ()) {
            if (qp.getLastChar () != '(') qp.append (',');
            qp.append (i.getName ());
        }
        qp.append (')');
        return qp;
    }
    
    private List<String> getInvalidObjectNames (String type) throws SQLException {
        
        List<String> result = new ArrayList<> ();
        
        forEach (new QP ("SELECT object_name FROM user_objects WHERE object_type=? AND status=?", type, "INVALID"), (rs) -> {
            result.add (rs.getString (1));
        });
        
        return result;
        
    }
    
    private void compile (String type, String name) throws SQLException {
        d0 ("ALTER " + type + ' ' + name + " COMPILE");
    }
    
    @Override
    protected void checkModel () throws SQLException {

        try {
            d0 ("PURGE RECYCLEBIN");
        }
        catch (SQLException ex) {
            logger.log (Level.SEVERE, "Cannot PURGE RECYCLEBIN", ex);
        }
        
        int oldNBrokenViews = Integer.MAX_VALUE;
        Set<String> fixedViewNames = new HashSet<> ();
        
        while (true) {

            List<String> brokenViewNames = getInvalidObjectNames ("VIEW");

            if (brokenViewNames.isEmpty ()) break;

            if (brokenViewNames.size () == oldNBrokenViews) throw new IllegalStateException ("Cannot compile views: " + brokenViewNames);

            for (String name: brokenViewNames) compile ("VIEW", name);

            fixedViewNames.addAll (brokenViewNames);

            oldNBrokenViews = brokenViewNames.size ();

        }
        
        if (!fixedViewNames.isEmpty ()) {
            
            PhysicalModel ex = getExistingModel ();
            
            for (String name: fixedViewNames) {

                View viewToBe = (View) getModel ().get (name);
                
                if (viewToBe == null) continue;

                for (Col col: viewToBe.getColumns ().values ())
                    
                    if (!DB.eq (col.getRemark (), ex.getRemark (col)))
                        
                        comment (viewToBe, col);
                
            }
            
        }

        for (String name: getInvalidObjectNames ("TRIGGER")) compile ("TRIGGER", name);

        forFirst (new QP ("SELECT NAME, TEXT, TYPE FROM USER_ERRORS"), rs -> {
            throw new IllegalStateException (rs.getString ("TYPE") + " " + rs.getString ("NAME") + ": " + rs.getString ("TEXT"));
        });
        
    };
    
    public enum TemporalityType {
        GLOBAL,
        PRIVATE
    }
    
    public enum TemporalityRowsAction {
        DELETE,
        PRESERVE
    }
    

}