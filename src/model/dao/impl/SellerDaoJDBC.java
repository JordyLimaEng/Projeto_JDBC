package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection conn;

	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(Seller obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub

	}

	@Override
	public Seller findById(Integer id) {//retornar seller por id
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?"
					);
			st.setInt(1, id);
			rs = st.executeQuery();//assim que recebe, aponta para a pos 0, que n tem nada
			
			if(rs.next()) {//se veio algum resultado na requisição
				//instanciar dept
				Department dep = instantiateDepartment(rs);
				//instanciar seller
				Seller Obj = instantiateSeller(rs, dep);
				//devolve o objeto com as requisições
				return Obj;
			}
			
			return null;//se não recebeu nada, retorna nuull
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);;
		}
		
		
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {//como a fução é insanciada em um local em que já é tratado a exception
																						//Aqui não é tratada a exceção
		Seller Obj = new Seller();
		Obj.setId(rs.getInt("Id"));
		Obj.setName(rs.getString("Name"));
		Obj.setEmail(rs.getString("Email"));
		Obj.setBaseSalary(rs.getDouble("BaseSalary"));
		Obj.setBirthDate(rs.getDate("BirthDate"));
		Obj.setDepartment(dep);
		return Obj;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {//como a fução é insanciada em um local em que já é tratado a exception
																				//Aqui não é tratada a exceção
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

}
