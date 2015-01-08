package dblab.shop.transactions.sql;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * JDBC Aufgabe 4b
 * 
 * Stellt einfache Ausgabefunktionen für ResultSet-Instanzen zur Verfügung.
 *
 * Die Funktion resultToCsv(), zum Export ins CSV Format, muss implementiert
 * werden.
 * 
 * Zum Testen bitte die main-Methode der Klasse CustomerSupplierRelations
 * ausführen.
 *
 */
public class Output {

	/**
	 * Die maximale Spaltenbreite für formatierte Ausgaben.
	 */
	private static final int MAX_COL_WIDTH = 30;

	/**
	 * Gibt zurück, ob ein Typ (aus java.sql.Types) in Anführungszeichen
	 * dargestellt werden sollte.
	 * 
	 * @param type
	 *            Alle aus Types.*
	 * @return False numerische Werte, für alles andere true
	 */
	private static boolean isQuotedType(int type) {
		// Numerische Werte nicht in Anführungszeichen
		switch (type) {
		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.REAL:
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
		case Types.NULL:
		case Types.BOOLEAN:
			return false;
		}
		// Alles andere in Anführungszeichen
		return true;
	}

	/**
	 * Gibt eine gegebene ResultSet-Instanz im CSV-Format aus.
	 *
	 * Felder werden durch Semikolon getrennt, quotierte Typen werden "getrimmt"
	 * (String.trim()) und in doppelte Anführungszeichen gestellt.
	 * 
	 * @param rs
	 *            Die ResultSet Instanz, die auszugeben ist
	 * @param out
	 *            Die PrintStream Instanz, auf die ausgegeben wird, z.B.
	 *            System.out
	 * @throws SQLException
	 *             Im Falle von Verbindungsproblemen
	 */
	public static void resultToCsv(ResultSet rs, PrintStream out)
			throws SQLException {

		ResultSetMetaData meta = rs.getMetaData();
		int columns = meta.getColumnCount();
		boolean[] leftAligned = new boolean[columns]; // Feld speichert für jede
														// Spalte ob sie quoted
														// ist

		// Tabellenkopf
		for (int column = 1; column <= columns; column++) {
			leftAligned[column - 1] = isQuotedType(meta.getColumnType(column));
			out.printf(meta.getColumnLabel(column)
					+ (column == columns ? "\n" : ";"));
		}

		// Zeilen
		while (rs.next()) {
			for (int column = 1; column <= columns; column++) {

				if (leftAligned[column - 1]) {
					// Quotierte Typen werden "getrimmt" (String.trim()) und in
					// doppelte Anführungszeichen gestellt.
					out.printf("\""
							+ (rs.getString(column) == null ? "" : rs
									.getString(column).trim()) + "\""
							+ (column == columns ? "\n" : ";"));
				} else {
					out.printf((rs.getString(column) == null ? "" : rs
							.getString(column).trim())
							+ (column == columns ? "\n" : ";"));
				}
			}
		}
	}

	/**
	 * Gibt ein ResultSet ähnlich wie ein SQL Kommandozeilen Client aus.
	 *
	 * Numerische Datentypen sind rechtsbündig, andere (in Anführungszeichen
	 * stehende) linksbündig ausgerichtet.
	 *
	 * Diese Funktion beachtet MAX_COL_WIDTH als Grenze für die maximale
	 * Spaltenbreite. Das heißt Felder, die breiter sind, werden bei der
	 * Darstellung abgeschnitten.
	 * 
	 * @param rs
	 *            Eine ResultSet Instanz
	 * @param out
	 *            Die PrintStream Instanz zur Ausgabe, z.B. System.out
	 * @throws SQLException
	 *             Im Falle von Verbindungsproblemen
	 */
	public static void printResultTable(ResultSet rs, PrintStream out)
			throws SQLException {
		// Erstellen eines horizontalen Abstandhalters in ausreichender Länge
		String horizSeparator = "--------------------------------------------";
		while (horizSeparator.length() < Output.MAX_COL_WIDTH) {
			horizSeparator += horizSeparator;
		}

		// Ausgabe der Überschriften und Ausrichtung und Breite der Spalten
		// ermitteln
		ResultSetMetaData meta = rs.getMetaData();
		int columns = meta.getColumnCount();
		int[] width = new int[columns];
		boolean[] leftAligned = new boolean[columns];
		for (int column = 1; column <= columns; column++) {
			leftAligned[column - 1] = isQuotedType(meta.getColumnType(column));
			width[column - 1] = meta.getColumnDisplaySize(column);
			width[column - 1] = width[column - 1] > Output.MAX_COL_WIDTH ? Output.MAX_COL_WIDTH
					: width[column - 1];
			out.printf((column > 1 ? "| " : " ") + "%-" + width[column - 1]
					+ "." + width[column - 1] + "s ",
					meta.getColumnLabel(column));
		}
		out.println();

		// Ausgabe horizontaler Abstandhalter
		for (int column = 1; column <= columns; column++) {
			out.printf((column > 1 ? "+-" : "-") + "%-" + width[column - 1]
					+ "." + width[column - 1] + "s-", horizSeparator);
		}
		out.println();

		// Ausgabe aller Zeilen aus ResultSet
		int rows = 0;
		while (rs.next()) {
			for (int column = 1; column <= columns; column++) {
				String cell = rs.getString(column);
				out.printf((column > 1 ? "| " : " ")
						+ (leftAligned[column - 1] ? "%-" : "%")
						+ width[column - 1] + "." + width[column - 1] + "s ",
						(cell != null ? cell : ""));
			}
			rows++;
			out.println();
		}
		out.printf("(%d rows)%n%n", rows);
	}
}
