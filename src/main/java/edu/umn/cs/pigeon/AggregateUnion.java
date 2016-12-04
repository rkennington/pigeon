/*******************************************************************
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
//import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.FuncSpec;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

//import edu.umn.cs.pigeon.DataByteArray;
import edu.umn.cs.pigeon.ESRIGeometryParser;
//import edu.umn.cs.pigeon.Union.Intermed;
//import edu.umn.cs.pigeon.Union.Final;
//import edu.umn.cs.pigeon.Union.Initial;

// Further Reading:
// http://svn.apache.org/viewvc/pig/trunk/src/org/apache/pig/builtin/SUM.java?view=markup
// http://svn.apache.org/viewvc/pig/trunk/src/org/apache/pig/builtin/COUNT.java?view=markup
//https://pig.apache.org/docs/r0.9.1/udf.html#eval-functions-write

/**
 * @author RKennington
 * 
 *         This utilizes the Accumulator interface to enhance memory usage of it
 *         as a MapReduce job. It is an enhancement to Pigeon union() function
 *         that will have performance issues for very large aggregations. It is
 *         modeled after the Pigeon Union and Pig COUNT/SUM classes.
 * 
 *         In Pig, problems with memory usage can occur when data, which results
 *         from a group or cogroup operation, needs to be placed in a bag and
 *         passed in its entirety to a UDF.
 * 
 *         This problem is partially addressed by Algebraic UDFs that use the
 *         combiner and can deal with data being passed to them incrementally
 *         during different processing phases (map, combiner, and reduce).
 *         However, there are a number of UDFs that are not Algebraic, don't use
 *         the combiner, but still don’t need to be given all data at once.
 * 
 *         The new Accumulator interface is designed to decrease memory usage by
 *         targeting such UDFs. For the functions that implement this interface,
 *         Pig guarantees that the data for the same key is passed continuously
 *         but in small increments.
 */
public class AggregateUnion extends EvalFunc<DataByteArray> implements
		Algebraic, Accumulator<DataByteArray> {

	private static TupleFactory mTupleFactory = TupleFactory.getInstance();

	private static final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

	private OGCGeometry intermediatePolygon = null;
	
	private static String temp = null;

	// @Override
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.Accumulator#accumulate(org.apache.pig.data.Tuple)
	 * 
	 * The accumulate allows Pig to provide smaller sets of DatBags to be
	 * handled during processing.
	 */
	public void accumulate(Tuple input) throws IOException {

		if (intermediatePolygon == null) {
			System.out.print("accumulate: initialized");
			intermediatePolygon = geometryParser.parseGeom("POLYGON EMPTY");
		}
		temp = intermediatePolygon.asText();
		try {
			intermediatePolygon = union(input, intermediatePolygon);
		} catch (ExecException ee) {
			throw ee;
		} catch (Exception e) {
			int errCode = 2106;
			String msg = "Error while computing aggregate polygon in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}

	/**
	 * Implemented for reuse by the accumulator() and exec(). Modeled after
	 * INTEGER and Pigeon Union.
	 * 
	 * @param input
	 * @param intermediatePolygon
	 * @return OGCGeometry to the accumulate() and exec() methods.
	 * @throws ExecException
	 */
	static protected OGCGeometry union(Tuple input,
			OGCGeometry intermediatePolygon) throws ExecException {

		OGCGeometry parsedGeom = null;
		ArrayList<OGCGeometry> all_geoms = new ArrayList<OGCGeometry>();

		if (intermediatePolygon != null) {
			// Always pass a null value from exec() as it does not need an to
			// keep track of its state. To preserve state of the accumulator
			// always pass its intermediatePolygon to this union() method.
			all_geoms.add(intermediatePolygon);
		}

		if (input != null) {
			DataBag values = (DataBag) input.get(0);
			if (values != null && values.size() != 0) {
				for (Tuple one_geom : values) {
					parsedGeom = geometryParser.parseGeom(one_geom.get(0));
					temp = parsedGeom.asText();
					all_geoms.add(parsedGeom);
				}
			} else {
				return null;
			}
		}

		// Do a union of all_geometries in the recommended way (using buffer(0))
		OGCGeometryCollection geom_collection = new OGCConcreteGeometryCollection(
				all_geoms, all_geoms.get(0).getEsriSpatialReference());
		OGCGeometry parsedGeom2 = geom_collection.union(all_geoms.get(0));
		temp = parsedGeom2.asText();
		return geom_collection.union(all_geoms.get(0));
	}
	
	static protected OGCGeometry union(Tuple input) throws ExecException {
	    DataBag values = (DataBag)input.get(0);
	    if (values.size() == 0)
	      return null;
	    ArrayList<OGCGeometry> all_geoms = new ArrayList<OGCGeometry>();
	    for (Tuple one_geom : values) {
	      OGCGeometry parsedGeom = geometryParser.parseGeom(one_geom.get(0));
	      all_geoms.add(parsedGeom);
	    }
	    
	    // Do a union of all_geometries in the recommended way (using buffer(0))
	    OGCGeometryCollection geom_collection = new OGCConcreteGeometryCollection(
	        all_geoms, all_geoms.get(0).getEsriSpatialReference());
	    return geom_collection.union(all_geoms.get(0));
	  }


	// @Override
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.Accumulator#cleanup()
	 * 
	 * The cleanup function is called after getValue but before the next value
	 * is processed.
	 */
	public void cleanup() {
		temp = intermediatePolygon.asText();
		intermediatePolygon = null;
	}

	// @Override
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.Accumulator#getValue()
	 * 
	 * The getValue function is called after all the tuples for a particular key
	 * have been processed to retrieve the final value.
	 */
	public DataByteArray getValue() {
		String temp = intermediatePolygon.asText();
		return new DataByteArray(intermediatePolygon.asBinary().array());
	}

	// @Override
	public String getInitial() {
		return Initial.class.getName();
	}

	// @Override
	public String getIntermed() {
		return Intermed.class.getName();
	}

	// @Override
	public String getFinal() {
		return Final.class.getName();
	}

	/**
	 * @author RKennington
	 * 
	 */
	static public class Initial extends EvalFunc<Tuple> {
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
		 * 
		 * Since Initial is guaranteed to be called only in the map, it will be
		 * called with an input of a bag with a single tuple - the count should
		 * always be 1 if bag is non empty.
		 */
		@Override
		public Tuple exec(Tuple input) throws IOException {

			// Retrieve the first element (tuple) in the given bag
			// return ((DataBag) input.get(0)).iterator().next();

			DataBag bag = (DataBag) input.get(0);
			Iterator it = bag.iterator();
			if (it.hasNext()) {
				Tuple t = (Tuple) it.next();
				if (t != null && t.size() > 0 && t.get(0) != null)
					
					temp = t.get(0).toString();
				return mTupleFactory.newTuple(
						new DataByteArray(union(input, null).asBinary().array()));			}
			return mTupleFactory.newTuple(null);
		}
	}

	static public class Intermed extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			return mTupleFactory.newTuple(
					new DataByteArray(union(input, null).asBinary().array()));
		}
	}

	static public class Final extends EvalFunc<DataByteArray> {
		@Override
		public DataByteArray exec(Tuple input) throws IOException {
			return new DataByteArray(union(input, null).asBinary().array());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public DataByteArray exec(Tuple input) throws IOException {

		OGCGeometry unionResult = union(input, null);
		if (unionResult == null) {
			return null;
		} else {
			return new DataByteArray(unionResult.asBinary().array());
		}
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.BAG));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	}
}