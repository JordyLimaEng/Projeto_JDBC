package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("INSERT INTO seller " + "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES " + "(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);

			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());

			int rowsAffected = st.executeUpdate();

			if (rowsAffected > 0) {// quer dizer que inseriu corretamente
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Error! No rows affected!!");
			}

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller obj) {

		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("UPDATE seller "
					+ "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? " + "WHERE Id = ?");

			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			st.setInt(6, obj.getId());

			st.executeUpdate();

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {

		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("DELETE FROM seller WHERE id = ?");

			st.setInt(1, id);

			st.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public Seller findById(Integer id) {// retornar seller por id
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE seller.Id = ?");
			st.setInt(1, id);
			rs = st.executeQuery();// assim que recebe, aponta para a pos 0, que n tem nada

			if (rs.next()) {// se veio algum resultado na requisição
				// instanciar dept
				Department dep = instantiateDepartment(rs);
				// instanciar seller
				Seller Obj = instantiateSeller(rs, dep);
				// devolve o objeto com as requisições
				return Obj;
			}

			return null;// se não recebeu nada, retorna null
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			;
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {// como a fução é insanciada em
																						// um local em que já é tratado
																						// a exception
																						// Aqui não é tratada a exceção
		Seller Obj = new Seller();
		Obj.setId(rs.getInt("Id"));
		Obj.setName(rs.getString("Name"));
		Obj.setEmail(rs.getString("Email"));
		Obj.setBaseSalary(rs.getDouble("BaseSalary"));
		Obj.setBirthDate(rs.getDate("BirthDate"));
		Obj.setDepartment(dep);
		return Obj;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {// como a fução é insanciada em um local
																				// em que já é tratado a exception
																				// Aqui não é tratada a exceção
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "ORDER BY Name");

			rs = st.executeQuery();// assim que recebe, aponta para a pos 0, que n tem nada

			List<Seller> list = new ArrayList<>();

			Map<Integer, Department> map = new HashMap<>(); // map aqui serve para evitar que as requisições venham
															// em obj's separados, mas sim atrelados
															// guarda todos os depts q forem instanciados

			while (rs.next()) {// pode ser mais ou igual a zero, logo temos q percorrer
				// verifica se o dep ja existe no map, se não existe, retorna nulo.
				Department dep = map.get(rs.getInt("DepartmentId"));

				if (dep == null) {// não existem logo instancia
					dep = instantiateDepartment(rs);
					// salva dep com a key sendo a id do departamento
					map.put(rs.getInt("DepartmentId"), dep);
				}

				// instanciar seller
				Seller Obj = instantiateSeller(rs, dep);
				// devolve o objeto com as requisições
				list.add(Obj);
			}
			return list;// se não recebeu nada, retorna nuull
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE DepartmentId = ? " + "ORDER BY Name");
			st.setInt(1, department.getId());
			rs = st.executeQuery();// assim que recebe, aponta para a pos 0, que n tem nada

			List<Seller> list = new ArrayList<>();

			Map<Integer, Department> map = new HashMap<>(); // map aqui serve para evitar que as requisições venham
															// em obj's separados, mas sim atrelados
															// guarda todos os depts q forem instanciados

			while (rs.next()) {// pode ser mais ou igual a zero, logo temos q percorrer
				// verifica se o dep ja existe no map, se não existe, retorna nulo.
				Department dep = map.get(rs.getInt("DepartmentId"));

				if (dep == null) {// não existem logo instancia
					dep = instantiateDepartment(rs);
					// salva dep com a key sendo a id do departamento
					map.put(rs.getInt("DepartmentId"), dep);
				}

				// instanciar seller
				Seller Obj = instantiateSeller(rs, dep);
				// devolve o objeto com as requisições
				list.add(Obj);
			}
			return list;// se não recebeu nada, retorna nuull
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
}
