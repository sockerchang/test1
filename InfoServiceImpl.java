package com.cares.gm.service.a.a;

import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.mail.MessagingException;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.STRUCT;

import org.apache.commons.beanutils.PropertyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.orm.hibernate3.HibernateTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import cn.sh.cares.framework.query.BeanQueryCondition;
import cn.sh.cares.framework.query.Sorter;
import cn.sh.cares.framework.query.hql.MatchMode;
import cn.sh.cares.framework.query.hql.Restrictions;
import cn.sh.cares.framework.service.ServiceException;
import cn.sh.cares.framework.system.SystemConfiguration;
import cn.sh.cares.framework.utils.DateUtils;
import cn.sh.cares.framework.utils.StringUtils;
import cn.sh.cares.framework.vo.BasePo;
import cn.sh.cares.pagination.OrderablePagination;
import cn.sh.cares.velocity.VelocityHelper;

import com.cares.b2b.vo.aa.TrCompVO;
import com.cares.b2b.vo.aa.Trcompemp;
import com.cares.b2b.vo.aa.TrsubcompB2g;
import com.cares.gm.service.a.c.DistributeService;
import com.cares.gm.service.a.e.RelationAssessService;
import com.cares.gm.service.a.f.ContractService;
import com.cares.gm.service.a.f.ContractServiceImpl;
import com.cares.gm.service.common.GmServiceImpl;
import com.cares.gm.service.common.PassWord;
import com.cares.gm.vo.a.f.KamValueAddedServiceMap;
import com.cares.gm.vo.a.f.KamValueAddedServiceMapId;
import com.cares.gm.vo.a.f.TBasispolicyPerbonusMap;
import com.cares.gm.vo.a.f.TKam;
import com.cares.gm.vo.a.f.TKamAgentMap;
import com.cares.gm.vo.a.f.TKamAgentMapId;
import com.cares.gm.vo.a.f.TKamBasispolicyMap;
import com.cares.gm.vo.a.f.TKamBizMap;
import com.cares.gm.vo.a.f.TKamBizMapId;
import com.cares.gm.vo.a.f.TKamBiztype;
import com.cares.gm.vo.a.f.TKamCmpMap;
import com.cares.gm.vo.a.f.TKamCmpMapId;
import com.cares.gm.vo.a.f.TKamCntlMap;
import com.cares.gm.vo.a.f.TKamCntlMapId;
import com.cares.gm.vo.a.f.TKamCompany;
import com.cares.gm.vo.a.f.TKamContact;
import com.cares.gm.vo.a.f.TKamContactMap;
import com.cares.gm.vo.a.f.TKamContactMapId;
import com.cares.gm.vo.a.f.TKamContract;
import com.cares.gm.vo.a.f.TKamCustomer;
import com.cares.gm.vo.a.f.TKamImage;
import com.cares.gm.vo.a.f.TKamLog;
import com.cares.gm.vo.a.f.TPolicyPostbonusMap;
import com.cares.gm.vo.a.f.TPriAccount;
import com.cares.gm.vo.a.f.TPriContact;
import com.cares.gm.vo.a.f.TPriDept;
import com.cares.gm.vo.a.f.TSpaceDiscount;
import com.cares.gm.vo.a.f.TSpecialAirline;
import com.cares.gm.vo.a.f.TSubCompany;
import com.cares.gm.vo.a.f.TUseAwardsPerBonus;
import com.cares.gm.vo.a.f.TUseAwardsPerBonusCanbin;
import com.cares.gm.vo.a.f.TUseContractAwardsMap;
import com.cares.gm.vo.a.f.ValueAddedService;
import com.cares.gm.vo.common.TFlowCM;
import com.cares.gm.vo.common.TFlowNodeCM;
import com.cares.gm.vo.common.TFlowOperateLogCM;
import com.cares.gm.vo.common.TKamCM;
import com.cares.gm.vo.common.TKamCompanyCM;
import com.cares.gm.vo.common.TPriAccountCM;
import com.cares.gm.vo.common.TPriDeptCM;
import com.cares.gm.vo.common.TPriGroupCM;
import com.cares.gm.vo.common.TPriRoleCM;
import com.cares.gm.web.dto.a.aa.TKamDto;
import com.cares.gm.web.dto.a.q.TKamUatpReportDto;
import com.cares.gm.web.dto.cb.AgentDto;
import com.cea.callcenter.ws.kms.KmsGroupSync;
import com.cea.callcenter.ws.kms.KmsGroupSyncImplService;
import com.cea.callcenter.ws.kmsnew.AddServiceNew;
import com.cea.callcenter.ws.kmsnew.GroupKamInfoNew;
import com.cea.callcenter.ws.kmsnew.KamPolicyNew;
import com.cea.callcenter.ws.kmsnew.KmsGroupSyncNew;
import com.cea.callcenter.ws.kmsnew.KmsGroupSyncNewImplService;
import com.cea.callcenter.ws.kmsnew.SpecialAirlineDiscountNew;
import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;

@Component("InfoService")
public class InfoServiceImpl extends GmServiceImpl implements InfoService {
	@Autowired
	ContractService contractservice;

	@Autowired
	private DistributeService distributeService;

	@Autowired
	private RelationAssessService relationAssessService;

	private static Map<Integer,String> spaceMap = new HashMap<Integer, String>();
	static{
		spaceMap.put(0, "F");
		spaceMap.put(1, "C");
		spaceMap.put(2, "Y");
		spaceMap.put(3, "P");
		spaceMap.put(4, "J");
		spaceMap.put(5, "Z");
		spaceMap.put(6, "K");
		spaceMap.put(7, "B");
		spaceMap.put(8, "E");
		spaceMap.put(9, "H");
		spaceMap.put(10, "L");
		spaceMap.put(11, "M");
	}
	
	@Autowired
	@Qualifier("hibernateTemplateB2b")
	private HibernateTemplate hibernate;
	
	private TKamLog logsucess = new TKamLog();
	private TKamLog logfaild = new TKamLog();
	
	@SuppressWarnings("unchecked")
	private List<Long> getKamByManagerId(String managername) {
		List<Long> kamids = new ArrayList<Long>();
		String hql = "select  account.accountid from TPriAccount account where account.namecn ='"
				+ managername + "'"; 
		List<Long> list = getEntityManager().queryForListByHql(hql);
		if (list.size() != 0) {
			hql = "from TKamCompanyCM company where company.accountId="
					+ list.get(0);
			if(list.size()>1){
				for(int i=1;i<list.size();i++)
					hql  = hql+" or company.accountId="+list.get(i);
			}
			List<TKamCompanyCM> list2 = getEntityManager().queryForListByHql(hql);
			for (int j = 0; j < list2.size(); j++) {
				kamids.add(list2.get(j).getKamId());
			}
		}
		return kamids;
	}

	 /* 根据条件查询前台注册大客户
	 * @param tkamDto
	 * @param p
	 * @param groupId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> queryTKamRegister(TKamDto tkamDto, OrderablePagination p,
			Long groupId) throws ServiceException {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);

//		bqc.addExpressions(Restrictions.like("kamno", "90", MatchMode.START));
//		bqc.addExpressions(Restrictions.not(Restrictions.like("kamno", "909", MatchMode.START)));
		bqc.addExpressions(Restrictions.and(Restrictions.like("kamno", "90", MatchMode.START),Restrictions.not(Restrictions.like("kamno", "909", MatchMode.START))));
		
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			List<Long> deptIdList = new ArrayList<Long>();
			for(TPriDeptCM d : deptList) {
				deptIdList.add(d.getME_departmentid());
			}
			bqc.addExpressions(Restrictions.in("TPriDept.departmentid", deptIdList));
		}else {
			return new ArrayList();
		}
		
		//1:按营业部查询
		String querydeptid = tkamDto.getDeptid();
		if (querydeptid != null && !querydeptid.equals("")) {
			bqc.addExpressions(Restrictions.equal("TPriDept.departmentid", Long.parseLong(querydeptid)));
		}
		
		
		//2:按代理人查询
		String queryagent = tkamDto.getAgent();
		if (StringUtils.hasText(queryagent)) {
			List<Long> list = relationAssessService.getKamByAgent(queryagent);
			String hql = "select map.TKam.kamid from TKamAgentMap map where map.TPriDept.delflag=0 and map.TPriDept.depttype=1 " +
					"and map.TPriDept.departmentid in (select t.dept.departmentid from TInfoIata t where t.iatacode like '%" + queryagent + "%')";
			List<String> listIata = getEntityManager().queryForListByHql(hql);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			}else if(null != listIata && listIata.size() != 0) {
				bqc.addExpressions(SuperRestrictions.in("kamid", hql));
			} else
				return new ArrayList();
		}
		
		//3:按联系人查询
		String querycontact = tkamDto.getContact();
		if (StringUtils.hasText(querycontact)) {
			List<Long> list = relationAssessService
					.getKamByCustomer(querycontact);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//4:按客户经理查询
		String queryaccount = tkamDto.getAccount();
		if (StringUtils.hasText(queryaccount)) {
			List<Long> list = getKamByManagerId(queryaccount);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//5:20091022 Add by PGQ,按照营业部下级组查询
		//按照该方式拼接的SQL语句，在In语句中超过1000个kmid，会查询出错
		String childGroupId=tkamDto.getDeptChildGroupId();
		if( StringUtils.hasText(childGroupId ) )
		{
			//从最内层开始分解
			//1、查询当前营业部下级组下的所有accountid
			//2、查询这些人所管理的companyid
			//3、查询这些公司所对应的kamid
			String sql = "select distinct m.kamid from t_kam_cmp_map  m where m.companyid in " +
					"		(" +
					"			select distinct c.companyid from t_kam_company c where c.accountid in" +
					"			(" +
					"				select distinct m.accountid from t_pri_role_group_account_map m where m.accountid is not null and m.groupid =? " + 
					"			)" +
					"		)";
			List<String> objParam=new ArrayList<String>();
			objParam.add(childGroupId);
			List<Object> kamiList = getEntityManager().queryForListBySql(sql, objParam.toArray() );
			if ( kamiList!=null && !kamiList.isEmpty() ) 
			{
				List<Long> newKamiList = new ArrayList<Long>();
				for(int i=0;i<kamiList.size();i++)
				{
					newKamiList.add( Long.parseLong( kamiList.get(i).toString() ) );
				}
				
				bqc.addExpressions(Restrictions.in("kamid", newKamiList));
			}
		}
		
		bqc.addExpressions(Restrictions.and(Restrictions.like("kamno", tkamDto
				.getKamNo(), MatchMode.START), Restrictions.equal(
				"companytype", tkamDto.getCompanyType()), Restrictions.equal(
				"agreementtype", tkamDto.getAgreementtype()), Restrictions
				.equal("delflag", Long.parseLong("0"))));
		bqc.addExpressions(Restrictions.or(Restrictions.like("kamnamecn",
				tkamDto.getKamName()), Restrictions.like("kamnameen", tkamDto
				.getKamName())));
		if (tkamDto.getStatus() == null) {
			bqc.addExpressions(Restrictions.and(Restrictions.notEqual("status",
					new Long(11)), Restrictions.greaterThan("status", new Long(
					2))));
		} else {
			bqc.addExpressions(Restrictions
					.equal("status", tkamDto.getStatus()));
		}
		bqc.addInitFields("TPriDept");
		Sorter[] sorters = p.getSorters();
		if (ObjectUtils.isEmpty(sorters)) {
			bqc.addSorters(Sorter.desc("kamid"));
		} else {
			bqc.addSorters(sorters);
		}
		List<TKam> kams = getEntityManager().queryWithPagination(bqc, p);

		// 给TKam加载account以便显示"客户经理"一列
		for (int i = 0; i < kams.size(); i++) {
			Long kamid = kams.get(i).getKamid();
			Long contractid = kams.get(i).getContractId();
			//根据kamid查询合同编号
//			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
//				+ kamid + " and t.contractstatus='0'";
			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
				+ kamid + " and t.contractid=" + contractid;
			List contractNos = getEntityManager().queryForListBySql(sql1);
			if(contractNos != null && contractNos.size()>0){
				if(contractNos.get(0) != null)
					kams.get(i).setContractNo(contractNos.get(0).toString());
			}
			
			String sql = "select map.COMPANYID from t_kam_cmp_map map where map.KAMID="
					+ kamid + " order by map.COMPANYID asc";
			List companyIds = getEntityManager().queryForListBySql(sql);
			Long companyId = null;
			if (companyIds.size() != 0)
				companyId = new Long(companyIds.get(0).toString());
			Long accountid = null;
			if (companyId != null)
				accountid = get(TKamCompanyCM.class, companyId).getAccountId();
			if (null != accountid) {
				TPriAccountCM account = get(TPriAccountCM.class, accountid);
				kams.get(i).setAccount(account);
			} else {
				TPriAccountCM account = new TPriAccountCM();
				account.setME_namecn("暂无");
				kams.get(i).setAccount(account);
			}
		}
		return kams;
	}
	
	
	
	
	
	
	/**
	 * 根据查询条件查询大客户信息
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> queryTKam(TKamDto tkamDto, OrderablePagination p,
			Long groupId) throws ServiceException {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);

		//修改为按groupid查询出所有的deptid 2010年10月18日 16:48:59
//		List<Long> groupidList = contractservice.getAllgroupByGroup(groupId);
//		if (null != groupidList && groupidList.size() != 0) {
//			bqc.addExpressions(Restrictions.in("groupid", groupidList));
//		} else {
//			return new ArrayList();
//		}
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			List<Long> deptIdList = new ArrayList<Long>();
			for(TPriDeptCM d : deptList) {
				deptIdList.add(d.getME_departmentid());
			}
			bqc.addExpressions(Restrictions.in("TPriDept.departmentid", deptIdList));
		}else {
			return new ArrayList();
		}
		
		//1:按营业部查询
//		String querydeptname = tkamDto.getDeptname();
		String querydeptid = tkamDto.getDeptid();
		if (querydeptid != null && !querydeptid.equals("")) {
//			String sql = "select t.departmentid from t_pri_dept t where t.depttype='0' and t.deptname like '%"
//					+ querydeptname + "%'";
//			List<Object> listlist = getEntityManager().queryForListBySql(sql);
//			List<Long> newlist1 = new ArrayList<Long>();
//			for (Object id : listlist) {
//				newlist1.add(Long.valueOf((((BigDecimal) id).toString())));
//			}
//			if (newlist1 != null && newlist1.size() > 0) {
//				bqc.addExpressions(Restrictions.in("TPriDept.departmentid",
//						newlist1));
//			}
			bqc.addExpressions(Restrictions.equal("TPriDept.departmentid", Long.parseLong(querydeptid)));
		}
		
		//2:按代理人查询
		String queryagent = tkamDto.getAgent();
		if (StringUtils.hasText(queryagent)) {
			List<Long> list = relationAssessService.getKamByAgent(queryagent);
			String hql = "select map.TKam.kamid from TKamAgentMap map where map.TPriDept.delflag=0 and map.TPriDept.depttype=1 " +
					"and map.TPriDept.departmentid in (select t.dept.departmentid from TInfoIata t where t.iatacode like '%" + queryagent + "%')";
			List<String> listIata = getEntityManager().queryForListByHql(hql);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			}else if(null != listIata && listIata.size() != 0) {
				bqc.addExpressions(SuperRestrictions.in("kamid", hql));
			} else
				return new ArrayList();
		}
		
		//3:按联系人查询
		String querycontact = tkamDto.getContact();
		if (StringUtils.hasText(querycontact)) {
			List<Long> list = relationAssessService
					.getKamByCustomer(querycontact);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//4:按客户经理查询
		String queryaccount = tkamDto.getAccount();
		if (StringUtils.hasText(queryaccount)) {
			List<Long> list = getKamByManagerId(queryaccount);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//5:20091022 Add by PGQ,按照营业部下级组查询
		//按照该方式拼接的SQL语句，在In语句中超过1000个kmid，会查询出错
		String childGroupId=tkamDto.getDeptChildGroupId();
		if( StringUtils.hasText(childGroupId ) )
		{
			//从最内层开始分解
			//1、查询当前营业部下级组下的所有accountid
			//2、查询这些人所管理的companyid
			//3、查询这些公司所对应的kamid
			String sql = "select distinct m.kamid from t_kam_cmp_map  m where m.companyid in " +
					"		(" +
					"			select distinct c.companyid from t_kam_company c where c.accountid in" +
					"			(" +
					"				select distinct m.accountid from t_pri_role_group_account_map m where m.accountid is not null and m.groupid =? " + 
					"			)" +
					"		)";
			List<String> objParam=new ArrayList<String>();
			objParam.add(childGroupId);
			List<Object> kamiList = getEntityManager().queryForListBySql(sql, objParam.toArray() );
			if ( kamiList!=null && !kamiList.isEmpty() ) 
			{
				List<Long> newKamiList = new ArrayList<Long>();
				for(int i=0;i<kamiList.size();i++)
				{
					newKamiList.add( Long.parseLong( kamiList.get(i).toString() ) );
				}
				
				bqc.addExpressions(Restrictions.in("kamid", newKamiList));
			}
		}
		
		bqc.addExpressions(Restrictions.and(Restrictions.like("kamno", tkamDto
				.getKamNo(), MatchMode.START), Restrictions.equal(
				"companytype", tkamDto.getCompanyType()), Restrictions.equal(
				"agreementtype", tkamDto.getAgreementtype()), Restrictions
				.equal("delflag", Long.parseLong("0"))));
		bqc.addExpressions(Restrictions.or(Restrictions.like("kamnamecn",
				tkamDto.getKamName()), Restrictions.like("kamnameen", tkamDto
				.getKamName())));
		if (tkamDto.getStatus() == null) {
			bqc.addExpressions(Restrictions.and(Restrictions.notEqual("status",
					new Long(11)), Restrictions.greaterThan("status", new Long(
					2))));
		} else {
			bqc.addExpressions(Restrictions
					.equal("status", tkamDto.getStatus()));
		}
		bqc.addInitFields("TPriDept");
		Sorter[] sorters = p.getSorters();
		if (ObjectUtils.isEmpty(sorters)) {
			bqc.addSorters(Sorter.desc("kamid"));
		} else {
			bqc.addSorters(sorters);
		}
		List<TKam> kams = getEntityManager().queryWithPagination(bqc, p);

		// 给TKam加载account以便显示"客户经理"一列
		for (int i = 0; i < kams.size(); i++) {
			Long kamid = kams.get(i).getKamid();
			Long contractid = kams.get(i).getContractId();
			//根据kamid查询合同编号
//			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
//				+ kamid + " and t.contractstatus='0'";
			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
				+ kamid + " and t.contractid=" + contractid;
			List contractNos = getEntityManager().queryForListBySql(sql1);
			if(contractNos != null && contractNos.size()>0){
				if(contractNos.get(0) != null)
					kams.get(i).setContractNo(contractNos.get(0).toString());
			}
			
			String sql = "select map.COMPANYID from t_kam_cmp_map map where map.KAMID="
					+ kamid + " order by map.COMPANYID asc";
			List companyIds = getEntityManager().queryForListBySql(sql);
			Long companyId = null;
			if (companyIds.size() != 0)
				companyId = new Long(companyIds.get(0).toString());
			Long accountid = null;
			if (companyId != null)
				accountid = get(TKamCompanyCM.class, companyId).getAccountId();
			if (null != accountid) {
				TPriAccountCM account = get(TPriAccountCM.class, accountid);
				kams.get(i).setAccount(account);
			} else {
				TPriAccountCM account = new TPriAccountCM();
				account.setME_namecn("暂无");
				kams.get(i).setAccount(account);
			}
		}
		return kams;
	}
	
	public String queryContractNobyContractId(Long contractid ,Long kamid)
	{
//		Long kamid = kams.get(i).getKamid();
//		Long contractid = kams.get(i).getContractId();
		//根据kamid查询合同编号
//		String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
//			+ kamid + " and t.contractstatus='0'";
		String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
			+ kamid + " and t.contractid=" + contractid;
		List contractNos = getEntityManager().queryForListBySql(sql1);
		if(contractNos != null && contractNos.size()>0){
			if(contractNos.get(0) != null)
				return contractNos.get(0).toString();
		}
		return null;
	}
	
	
	@SuppressWarnings("unchecked")
	public List<TKam> queryTKamAll(TKamDto tkamDto,
			Long groupId) throws ServiceException {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);

		//修改为按groupid查询出所有的deptid 2010年10月18日 16:48:59
//		List<Long> groupidList = contractservice.getAllgroupByGroup(groupId);
//		if (null != groupidList && groupidList.size() != 0) {
//			bqc.addExpressions(Restrictions.in("groupid", groupidList));
//		} else {
//			return new ArrayList();
//		}
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			List<Long> deptIdList = new ArrayList<Long>();
			for(TPriDeptCM d : deptList) {
				deptIdList.add(d.getME_departmentid());
			}
			bqc.addExpressions(Restrictions.in("TPriDept.departmentid", deptIdList));
		}else {
			return new ArrayList();
		}
		
		//1:按营业部查询
//		String querydeptname = tkamDto.getDeptname();
		String querydeptid = tkamDto.getDeptid();
		if (querydeptid != null && !querydeptid.equals("")) {
//			String sql = "select t.departmentid from t_pri_dept t where t.depttype='0' and t.deptname like '%"
//					+ querydeptname + "%'";
//			List<Object> listlist = getEntityManager().queryForListBySql(sql);
//			List<Long> newlist1 = new ArrayList<Long>();
//			for (Object id : listlist) {
//				newlist1.add(Long.valueOf((((BigDecimal) id).toString())));
//			}
//			if (newlist1 != null && newlist1.size() > 0) {
//				bqc.addExpressions(Restrictions.in("TPriDept.departmentid",
//						newlist1));
//			}
			bqc.addExpressions(Restrictions.equal("TPriDept.departmentid", Long.parseLong(querydeptid)));
		}
		
		//2:按代理人查询
		String queryagent = tkamDto.getAgent();
		if (StringUtils.hasText(queryagent)) {
			List<Long> list = relationAssessService.getKamByAgent(queryagent);
			String hql = "select map.TKam.kamid from TKamAgentMap map where map.TPriDept.delflag=0 and map.TPriDept.depttype=1 " +
					"and map.TPriDept.departmentid in (select t.dept.departmentid from TInfoIata t where t.iatacode like '%" + queryagent + "%')";
			List<String> listIata = getEntityManager().queryForListByHql(hql);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			}else if(null != listIata && listIata.size() != 0) {
				bqc.addExpressions(SuperRestrictions.in("kamid", hql));
			} else
				return new ArrayList();
		}
		
		//3:按联系人查询
		String querycontact = tkamDto.getContact();
		if (StringUtils.hasText(querycontact)) {
			List<Long> list = relationAssessService
					.getKamByCustomer(querycontact);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//4:按客户经理查询
		String queryaccount = tkamDto.getAccount();
		if (StringUtils.hasText(queryaccount)) {
			List<Long> list = getKamByManagerId(queryaccount);
			if (null != list && list.size() != 0) {
				bqc.addExpressions(Restrictions.in("kamid", list));
			} else
				return new ArrayList();
		}
		
		//5:20091022 Add by PGQ,按照营业部下级组查询
		//按照该方式拼接的SQL语句，在In语句中超过1000个kmid，会查询出错
		String childGroupId=tkamDto.getDeptChildGroupId();
		if( StringUtils.hasText(childGroupId ) )
		{
			//从最内层开始分解
			//1、查询当前营业部下级组下的所有accountid
			//2、查询这些人所管理的companyid
			//3、查询这些公司所对应的kamid
			String sql = "select distinct m.kamid from t_kam_cmp_map  m where m.companyid in " +
					"		(" +
					"			select distinct c.companyid from t_kam_company c where c.accountid in" +
					"			(" +
					"				select distinct m.accountid from t_pri_role_group_account_map m where m.accountid is not null and m.groupid =? " + 
					"			)" +
					"		)";
			List<String> objParam=new ArrayList<String>();
			objParam.add(childGroupId);
			List<Object> kamiList = getEntityManager().queryForListBySql(sql, objParam.toArray() );
			if ( kamiList!=null && !kamiList.isEmpty() ) 
			{
				List<Long> newKamiList = new ArrayList<Long>();
				for(int i=0;i<kamiList.size();i++)
				{
					newKamiList.add( Long.parseLong( kamiList.get(i).toString() ) );
				}
				
				bqc.addExpressions(Restrictions.in("kamid", newKamiList));
			}
		}
		
		bqc.addExpressions(Restrictions.and(Restrictions.like("kamno", tkamDto
				.getKamNo(), MatchMode.START), Restrictions.equal(
				"companytype", tkamDto.getCompanyType()), Restrictions.equal(
				"agreementtype", tkamDto.getAgreementtype()), Restrictions
				.equal("delflag", Long.parseLong("0"))));
		bqc.addExpressions(Restrictions.or(Restrictions.like("kamnamecn",
				tkamDto.getKamName()), Restrictions.like("kamnameen", tkamDto
				.getKamName())));
		if (tkamDto.getStatus() == null) {
			bqc.addExpressions(Restrictions.and(Restrictions.notEqual("status",
					new Long(11)), Restrictions.greaterThan("status", new Long(
					2))));
		} else {
			bqc.addExpressions(Restrictions
					.equal("status", tkamDto.getStatus()));
		}
		bqc.addInitFields("TPriDept");
//		Sorter[] sorters = p.getSorters();
//		if (ObjectUtils.isEmpty(sorters)) {
			bqc.addSorters(Sorter.desc("kamid"));
//		} else {
//			bqc.addSorters(sorters);
//		}
		List<TKam> kams = getEntityManager().query(bqc);
		// 给TKam加载account以便显示"客户经理"一列
//		for (int i = 0; i < kams.size(); i++) {
//			Long kamid = kams.get(i).getKamid();
//			Long contractid = kams.get(i).getContractId();
//			//根据kamid查询合同编号
////			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
////				+ kamid + " and t.contractstatus='0'";
//			String sql1 = "select t.contractno from t_kam_contract t where t.kamid="
//				+ kamid + " and t.contractid=" + contractid;
//			List contractNos = getEntityManager().queryForListBySql(sql1);
//			if(contractNos != null && contractNos.size()>0){
//				if(contractNos.get(0) != null)
//					kams.get(i).setContractNo(contractNos.get(0).toString());
//			}
//			
//			String sql = "select map.COMPANYID from t_kam_cmp_map map where map.KAMID="
//					+ kamid + " order by map.COMPANYID asc";
//			List companyIds = getEntityManager().queryForListBySql(sql);
//			Long companyId = null;
//			if (companyIds.size() != 0)
//				companyId = new Long(companyIds.get(0).toString());
//			Long accountid = null;
//			if (companyId != null)
//				accountid = get(TKamCompanyCM.class, companyId).getAccountId();
//			if (null != accountid) {
//				TPriAccountCM account = get(TPriAccountCM.class, accountid);
//				kams.get(i).setAccount(account);
//			} else {
//				TPriAccountCM account = new TPriAccountCM();
//				account.setME_namecn("暂无");
//				kams.get(i).setAccount(account);
//			}
//		}
		return kams;
	}
	
	/**
	 * add By PGQ 20091028
	 * 根据营业部编号或营业部经理帐号，查找属于营业部或指定的营业部经理的客户编号
	 * 如果两个参数均不指定，则查询所有的大客户编号
	 * @param compId
	 * @param accountId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings("unchecked")
	public List<Long> getKmaNoFromDeptOrAccount(String compId,String accountId) throws ServiceException {
		String sql = " SELECT distinct k.kamno FROM t_kam k WHERE k.kamid IN " +
					"( " +
					"	SELECT kcm.kamid FROM t_kam_cmp_map kcm WHERE kcm.companyid IN " +
					"		(" +
					"			SELECT kc.companyid FROM t_kam_company kc WHERE 1=1 ";
		List<String> objParam=new ArrayList<String>();		
		
		//缺省查询所有集团客户的kamno
		if( StringUtils.hasText(compId ) ){
			sql +=" AND kc.departmentid=?";
			objParam.add( compId );  //查询本营业部的大客户编号
		}
		if ( StringUtils.hasText( accountId) ){
			sql +=" AND kc.accountid=?";
			objParam.add( accountId );  //查询本营业部的大客户编号
		}
		sql +="))"; //闭合SQL语句
		
		return getEntityManager().queryForListBySql(sql, objParam.toArray() );
	}
	
	// 新增大客户信息
	@SuppressWarnings("unchecked")
	public TKam doInsertTKam(String[] serviceid,String[] departmentid, String[] perbonusid,
			String[] postbonusid, TKam tkam, TKamDto tkamDto,
			List<TKamCustomer> tkamImportant, List<TKamCustomer> tkamContact,
			Long[] biztype, String[] contactid, List<TSpecialAirline> specialAirlines , Long rolecode, Long usercode)
			throws ServiceException {
		//辨识全功能
		String isGroupId = tkam.getIsGroupId();
		// 新增大客户信息表
		TPriDept dept = tkam.getTPriDept();
		tkam.setTPriDept(null);
		Date currentTime = getEntityManager().getDbTime();
		int temp = Integer.parseInt(tkam.getKamno()) % 6;// 客户编号除以6的余数
		String tkamno = tkam.getKamno() + String.valueOf(temp);// 把余数补到最后
		tkam.setKamno(tkamno);// 设置客户编号
		tkam.setOperatedate(currentTime);// 设置操作时间
		tkam.setCreatedate(currentTime);//设置创建时间--2011年5月20日 12:23:13
		
		if(tkam.getStatus() == null)//无流程录入的新增大客户(全功能录入)----此处status为已审核
			tkam.setStatus(new Long(3)); // 设置大客户当前状态
		tkam.setDelflag(new Long(0)); // 设置有效
		if("".equals(tkam.getCabinpermit()) || tkam.getCabinpermit() == null){
			tkam.setCabinpermit(new Long(0));
		}
//		if("".equals(tkam.getKuaiqian()) || tkam.getKuaiqian() == null){
//			tkam.setKuaiqian("0");
//		}
//		if("".equals(tkam.getKailian()) || tkam.getKailian() == null){
//			tkam.setKailian("0");
//		}
//		if("".equals(tkam.getUatp()) || tkam.getUatp() == null){
//			tkam.setUatp("0");
//		}
		getEntityManager().save(tkam); // 保存大客户信息
		Long kamid = tkam.getKamid(); // 取得当前新增大客户的ID
		// 新增大客户的公司信息
		TKamCompany company = new TKamCompany();
		company.setCompanyname(tkam.getKamnamecn());// 新增company的名称和大客户的中文名一样
		getEntityManager().save(company); // 保存大客户总公司信息
		Long companyid = company.getCompanyid(); // 取得新增company的ID
		// 保存中间表TKamCmpMap
		TKamCmpMap cmpmap = new TKamCmpMap();
		TKamCmpMapId cmpmapid = new TKamCmpMapId();
		cmpmapid.setTKam(getEntityManager().get(TKam.class, kamid));
		cmpmapid.setTKamCompany(getEntityManager().get(TKamCompany.class,
				companyid));
		cmpmap.setId(cmpmapid);
		getEntityManager().save(cmpmap);
		//hibernate.flush();
		// 新增奖励政策
		List<Long> pers = new ArrayList<Long>();
		if (perbonusid != null) {
			for (int i = 0; i < perbonusid.length; i++) {
				pers.add(new Long(perbonusid[i].trim()));
			}
		}
		List<Long> posts = new ArrayList<Long>();
		if (postbonusid != null) {
			for (int i = 0; i < postbonusid.length; i++) {
				posts.add(new Long(postbonusid[i].trim()));
			}
		}

		contractservice.doSelectPolicyByKamId(pers, posts, kamid, null);
		// 新增代理人
		List<Long> agent = new ArrayList<Long>();
		if (departmentid != null) {
			for (int i = 0; i < departmentid.length; i++) {
				agent.add(new Long(departmentid[i].trim()));
			}
			//过虑重复的id号
			for(int t=0;t<agent.size();t++){
				String sub1	= agent.get(t).toString();
				for(int r=t+1;r<agent.size();r++){
				  String sub2 = agent.get(r).toString();
					if(sub1.equals(sub2)){
						agent.remove(r);				
					}						
				}						
			}
		    //如果是两方，那么代理人默认加iataCode='08673556'的代理人
			if(tkam.getAgreementtype()== 0){
				//
				List templist = new ArrayList();
				String sql = "select departmentid from t_info_iata t where t.IATACODE='08673556' and t.departmentid is not null";
				templist = getEntityManager().queryForListBySql(sql);
				if(templist!=null && templist.size()>0){
					Long id = Long.valueOf(templist.get(0).toString());				
						if(!agent.contains(id)){
							agent.add(id);					
					}					
				}
				//added 2010-02-02 两方客户自动添加呼叫中心IATA进入信息表
				List temp2list = new ArrayList();
				String sql2 = "select departmentid from t_info_iata t where t.IATACODE='08677917' and t.departmentid is not null";
				temp2list = getEntityManager().queryForListBySql(sql2);
				if(temp2list!=null && temp2list.size()>0){
					Long id = Long.valueOf(temp2list.get(0).toString());				
						if(!agent.contains(id)){
							agent.add(id);					
					}					
				}
			}
			try {
				this.saveAgent(agent, kamid);
			} catch (Exception e) {

				e.printStackTrace();
			}
		}else{
		    //如果是两方，那么代理人默认加iataCode='08673556'的代理人
			if(tkam.getAgreementtype()== 0){
				List templist = new ArrayList();
				String sql = "select departmentid from t_info_iata t where t.IATACODE='08673556' and t.departmentid is not null";
				templist = getEntityManager().queryForListBySql(sql);
				if(templist!=null && templist.size()>0){
					Long id = Long.valueOf(templist.get(0).toString());				
						if(!agent.contains(id)){
							agent.add(id);					
					}					
				}
				List temp2list = new ArrayList();
				String sql2 = "select departmentid from t_info_iata t where t.IATACODE='08677917' and t.departmentid is not null";
				temp2list = getEntityManager().queryForListBySql(sql2);
				if(temp2list!=null && temp2list.size()>0){
					Long id = Long.valueOf(temp2list.get(0).toString());				
						if(!agent.contains(id)){
							agent.add(id);					
					}					
				}	
			}
			try {
				this.saveAgent(agent, kamid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 新增代理人联系人
		List<Long> contact = new ArrayList<Long>();
		if (contactid != null) {
			for (int i = 0; i < contactid.length; i++) {
				contact.add(new Long(contactid[i].trim()));
			}
			//过虑重复的id号
			for(int t=0;t<contact.size();t++){
				String sub1	= contact.get(t).toString();
				for(int r=t+1;r<contact.size();r++){
				  String sub2 = contact.get(r).toString();
					if(sub1.equals(sub2)){
						contact.remove(r);				
					}						
				}						
			}
			try {
				this.saveContact(contact, kamid);
			} catch (Exception e) {

				e.printStackTrace();
			}
		}
		// 新增主营业务
		List<Long> biz = new ArrayList<Long>();
		if (biztype != null && biztype.length > 0) {
			for (int i = 0; i < biztype.length; i++) {
				biz.add(new Long(biztype[i]));
			}
		}
		try {
			this.saveBiztype(biz, tkam.getKamid());

		} catch (Exception e) {
			e.printStackTrace();
		}
		//新增特殊航线备注
		if(specialAirlines!=null){
			for (int i = 0; i < specialAirlines.size(); i++) {
				TSpecialAirline sAirline = specialAirlines.get(i);
				List<TSpaceDiscount> spaceDiscounts = sAirline.getTSpaceDiscounts();
				for (int j = 0; j < spaceDiscounts.size(); j++) {
					if("on".equals(spaceDiscounts.get(j).getSpaceType())){
						spaceDiscounts.get(j).setSpaceType("true");
					}
				}
			}
			this.saveSpecialAirline(specialAirlines, kamid);
		}
		
		//新增增值服务
		List<Long> service = new ArrayList<Long>();
		if (serviceid != null && serviceid.length > 0) {
			for (int i = 0; i < serviceid.length; i++) {
				service.add(new Long(serviceid[i]));
			}
		}
		try {
			this.saveService(service, tkam.getKamid());

		} catch (Exception e) {
			e.printStackTrace();
		}
		// 新增大客户对应的关键人
		if (tkamImportant != null && tkamImportant.size() > 0) {
			for (TKamCustomer importantVO : tkamImportant) {
				TKamCustomer vo = new TKamCustomer();

				vo.setCustomertype(new Long(0));
				if (importantVO.getNamecn() == null || "".equals(importantVO.getNamecn())) {
					vo.setNamecn("无");
				} else {
					vo.setNamecn(importantVO.getNamecn());
				}
				vo.setDelflag(new Long(0));
				vo.setCustomertype(0l);
				vo.setCompanyid(companyid);
				vo.setEmail(importantVO.getEmail());
				vo.setIdcardno(importantVO.getIdcardno());
				vo.setPosition(importantVO.getPosition());
				vo.setPassportcode(importantVO.getPassportcode());
				vo.setTelephone(importantVO.getTelephone());
				vo.setTelephone2(importantVO.getTelephone2());
				vo.setMobile(importantVO.getMobile());
				vo.setOthercode(importantVO.getOthercode());
				vo.setFirstnamecn(importantVO.getFirstnamecn());
				vo.setLastnamecn(importantVO.getLastnamecn());
				vo.setNamecn(importantVO.getFirstnamecn() + importantVO.getLastnamecn());					
				getEntityManager().save(vo);

				Long customerid = vo.getCustomerid();
				TKamCntlMap cntlmap = new TKamCntlMap();
				TKamCntlMapId cntid = new TKamCntlMapId();
				cntid.setTKam(getEntityManager().get(TKam.class, kamid));
				cntid.setTKamCustomer(getEntityManager().get(TKamCustomer.class, customerid));
				cntlmap.setId(cntid);
				getEntityManager().save(cntlmap);
				//调用WebService往CC中插关键人及中间表数据
//				this.doInsertCustomerToCC(tkam, vo);
			}
		}
		// 新增大客户对应的联系人
		if (tkamContact != null && tkamContact.size() > 0) {
			for (TKamCustomer contactVO : tkamContact) {
				TKamContact contactVo = new TKamContact();
				contactVo.setCreateDate(currentTime);        //创建日期
				contactVo.setCreatorid(tkam.getCreatorid()); //创建人ID
				contactVo.setOperatorid(tkam.getOperateid());//操作人ID
				contactVo.setOperateDate(currentTime);       //操作日期
				contactVo.setNamecn(contactVO.getNamecn()); //中文姓名
				contactVo.setFirstnamecn(contactVO.getFirstnamecn());    //中文姓
				contactVo.setLastnamecn(contactVO.getLastnamecn());      //中文名
				contactVo.setDelflag(new Long(0));                       //删除标记，默认为0
				contactVo.setFax(contactVO.getOthercode());              //传真/其他
				contactVo.setTelephone(contactVO.getTelephone());        //电话
				contactVo.setMobile(contactVO.getMobile());              //手机
				contactVo.setIdcardno(contactVO.getIdcardno());          //身份证
				contactVo.setPassportcode(contactVO.getPassportcode());  //护照
				contactVo.setEmail(contactVO.getEmail());                //电子邮件
				contactVo.setPosition(contactVO.getPosition());          //职位
				contactVo.setTKam(tkam);                                 //关联的KAM
				
				getEntityManager().save(contactVo);
				
				//----------原有代码  开始------------------//
//				TKamCustomer vo = new TKamCustomer();
//
//				vo.setCustomertype(new Long(1));
//				if (contactVO.getNamecn() == null || "".equals(contactVO.getNamecn())) {
//					vo.setNamecn("无");
//				} else {
//					vo.setNamecn(contactVO.getNamecn());
//				}
//				vo.setDelflag(new Long(0));
//				vo.setCustomertype(1l);
//				vo.setCompanyid(companyid);
//				vo.setEmail(contactVO.getEmail());
//				vo.setIdcardno(contactVO.getIdcardno());
//				vo.setPosition(contactVO.getPosition());
//				vo.setPassportcode(contactVO.getPassportcode());
//				vo.setTelephone(contactVO.getTelephone());
//				vo.setTelephone2(contactVO.getTelephone2());
//				vo.setMobile(contactVO.getMobile());
//				vo.setOthercode(contactVO.getOthercode());
//				vo.setFirstnamecn(contactVO.getFirstnamecn());
//				vo.setLastnamecn(contactVO.getLastnamecn());
//				vo.setNamecn(contactVO.getLastnamecn() + contactVO.getFirstnamecn());	
//				getEntityManager().save(vo);
//
//				Long customerid = vo.getCustomerid();
//				TKamCntlMap cntlmap = new TKamCntlMap();
//				TKamCntlMapId cntid = new TKamCntlMapId();
//				cntid.setTKam(getEntityManager().get(TKam.class, kamid));
//				cntid.setTKamCustomer(getEntityManager().get(
//						TKamCustomer.class, customerid));
//				cntlmap.setId(cntid);
//				getEntityManager().save(cntlmap);
//				//调用WebService往CC中插关键人及中间表数据
//				
//				this.doInsertCustomerToCC(tkam, vo);

			}
		}
		// 纪录操作日志
		TKamLog log = new TKamLog();
		log.setTKam(tkam);
		log.setOperatorid(tkam.getOperateid());
		log.setOperatedate(currentTime);
		log.setOperatetype(new Long(2));
		getEntityManager().save(log);
		
		// 数据同步到B2G
		if(tkam.getStatus() == 7)//无流程录入的新增大客户(全功能录入)----此处status为已审批
			this.updateB2Bdata(tkam, ContractServiceImpl.getRandomPassWordW(6));

		// 分配营业部
		if (dept.getDepartmentid() != null) {
			
			if(!"approved".equals(isGroupId)) {
				distributeService.doDistributeDeptWithOutGroup(kamid, companyid, rolecode,
						usercode, dept, true);
			}else {//全功能
				distributeService.doDistributeDeptWithOutGroupQuan(kamid, companyid, rolecode,
						usercode, dept, true);
			}
		}
		//调用WebService往CC中插大客户信息
		doSyncKamTocc(tkam.getKamno());
//		Boolean logtemp = this.doInsertTKamToCC(tkam,specialAirlines);
		//调用成功纪录日志
//		if(logtemp==true){
////			TKamLog logsucess = new TKamLog();
//			logsucess.setTKam(tkam);
//			logsucess.setOperatorid(tkam.getOperateid());
//			logsucess.setOperatedate(currentTime);
//			logsucess.setOperatetype(new Long(7));
//			getEntityManager().save(logsucess);	
//		}
//		//调用失败纪录日志
//		else{
//			
//			logfaild.setTKam(tkam);
//			logfaild.setOperatorid(tkam.getOperateid());
//			logfaild.setOperatedate(currentTime);
//			logfaild.setOperatetype(new Long(8));
//			getEntityManager().save(logfaild);			
//		}
		return tkam;
	}
	
	/**
	 * 增加联系人信息
	 * @param tkam
	 * @param tkamContact
	 */
	public void doInsertTKamContact(TKam tkam,List<TKamCustomer> tkamContact){
		Date currentTime = getEntityManager().getDbTime();
		if (tkamContact != null && tkamContact.size() > 0) {
			for (TKamCustomer contactVO : tkamContact) {
				TKamContact contactVo = new TKamContact();
				contactVo.setCreateDate(currentTime);        //创建日期
				contactVo.setCreatorid(tkam.getCreatorid()); //创建人ID
				contactVo.setOperatorid(tkam.getOperateid());//操作人ID
				contactVo.setOperateDate(currentTime);       //操作日期
				contactVo.setNamecn(contactVO.getFirstnamecn()+contactVO.getLastnamecn()); //中文姓名
				contactVo.setFirstnamecn(contactVO.getFirstnamecn());    //中文姓
				contactVo.setLastnamecn(contactVO.getLastnamecn());      //中文名
				contactVo.setDelflag(new Long(0));                       //删除标记，默认为0
				contactVo.setFax(contactVO.getOthercode());                    //传真/其他
				contactVo.setTelephone(contactVO.getTelephone());        //电话
				contactVo.setMobile(contactVO.getMobile());              //手机
				contactVo.setIdcardno(contactVO.getIdcardno());          //身份证
				contactVo.setPassportcode(contactVO.getPassportcode());
				contactVo.setTKam(tkam);
				getEntityManager().save(contactVo);
			}
		}
	}
	
	//无流程录入的新增大客户信息 added by lingd
	@SuppressWarnings("unchecked")
	public TKam doInsertTKamApprove(String[] serviceid,String[] departmentid, String[] perbonusid,
			String[] postbonusid, TKam tkam, TKamDto tkamDto,
			List<TKamCustomer> tkamImportant, List<TKamCustomer> tkamContact,
			Long[] biztype, String[] contactid,List<TSpecialAirline> specialAirline, Long rolecode, Long usercode, Long groupid) throws ServiceException {
		//审批通过
		tkam.setStatus(7l);
		//增加流程节点(当前人的group)
		tkam.setActivenodeid(null);
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TFlowCM.class);
		TFlowCM flow = null;
		try {
			flow = contractservice.getFlowByGroupIdFlowStatus(groupid, 0L);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(null != flow) {
			bqc = BeanQueryCondition.forClass(TFlowNodeCM.class);
			bqc.addExpressions(Restrictions.equal("TFlow.ME_flowid", flow.getME_flowid()));
			List<TFlowNodeCM> listFlowNodes = getEntityManager().query(bqc);
			for(TFlowNodeCM vo : listFlowNodes) {
				if(vo.getME_name().indexOf(ContractServiceImpl.flowNodeIdSH) != -1) {
					tkam.setActivenodeid(vo.getME_flownodeid());
					break;
				}
			}
		}
		//调用原先的新增方法
		tkam = this.doInsertTKam(serviceid, departmentid, perbonusid, postbonusid, tkam, tkamDto, tkamImportant, tkamContact, biztype, contactid,specialAirline, rolecode, usercode);
		//增加审批历史--该情况下默认系统审批
		TFlowOperateLogCM tflow = new TFlowOperateLogCM();	
		tflow.setME_remark("系统用户审核通过");
		tflow.setME_operatorid(new Long(0));
		tflow.setME_operatedate(new Date());
		tflow.setME_operatetype(new Long(1));
		tflow.setFlowtype(new Long(0));
		tflow.setFlowtypeId(tkam.getKamid());
		getEntityManager().save(tflow);
		return tkam;
	}

	/**
	 * yangpeng 2012-06-19
	 * 更新客户信息
	 * @param tkam
	 */
	public void updateTKam(TKam tkam){
		getEntityManager().update(tkam);
	}
	
	// 更新大客户信息
	@SuppressWarnings("unchecked")
	public void doUpdateTKam(String[] serviceid ,String[] departmentid, String[] perbonusid,
			String[] postbonusid, TKam tkam, TKamDto tkamDto,
			List<TKamCustomer> tkamImportant, List<TKamCustomer> tkamContact,
			Long[] biztype, String[] contactid,List<TSpecialAirline> specialAirline, Long rolecode, Long usercode,
			TKamLog tkamLog) throws ServiceException {
		//标识全功能
		String isGroupId = tkam.getIsGroupId();
		
		TKam kam = getTKamById(tkam.getKamid());
		//在全功能中更新kamno
		if("approved".equals(isGroupId))
			kam.setKamno(tkam.getKamno());
		// 更新大客户主表信息
		//2010-06-07 wangym 开始
		kam.setUatp(tkam.getUatp());
		kam.setKuaiqian(tkam.getKuaiqian());
		kam.setKailian(tkam.getKailian());
		kam.setCabinpermit(tkam.getCabinpermit());
		//2010-06-07 wangym 结束
		kam.setAddress(tkam.getAddress());
		kam.setAcceptdate(tkam.getAcceptdate());
		kam.setActivenodeid(tkam.getActivenodeid());
		kam.setAgreementstatus(tkam.getAgreementstatus());
		kam.setAgreementtype(tkam.getAgreementtype());
		kam.setApplydate(tkam.getApplydate());
		kam.setBalancetype(tkam.getBalancetype());
		kam.setBonusdesc(tkam.getBonusdesc());
		kam.setBonuspercent(tkam.getBonuspercent());
		kam.setCompanytype(tkam.getCompanytype());
		kam.setCompanytypestr(tkam.getCompanytypestr());
		kam.setCreatedate(tkam.getCreatedate());
		kam.setDelflag(tkam.getDelflag());
		kam.setEmployeenumber(tkam.getEmployeenumber());
		kam.setExpditurelmt(tkam.getExpditurelmt());
		kam.setFax(tkam.getFax());
		kam.setFlowid(tkam.getFlowid());
		kam.setKamcategory(tkam.getKamcategory());
		kam.setKamcontinue(tkam.getKamcontinue());
		kam.setKamid(tkam.getKamid());
		kam.setKamlevel(tkam.getKamlevel());
		kam.setKamnamecn(tkam.getKamnamecn());
		kam.setKamnameen(tkam.getKamnameen());
		kam.setKamrank(tkam.getKamrank());
		kam.setKamrelationship(tkam.getKamrelationship());
		kam.setKamtype(tkam.getKamtype());
		kam.setTravelcost(tkam.getTravelcost());
		kam.setTravellocation(tkam.getTravellocation());
		kam.setWebsite(tkam.getWebsite());
		kam.setRemark(tkam.getRemark());
		kam.setRestcapital(tkam.getRestcapital());
		
		kam.setBusinessLicenseCode(tkam.getBusinessLicenseCode());
		kam.setOrganizationCode(tkam.getOrganizationCode());
		kam.setLastMainAmount(tkam.getLastMainAmount());
		
		kam.setLasttaxamount(tkam.getLasttaxamount());
		kam.setPostcode(tkam.getPostcode());
		Date currentTime = getEntityManager().getDbTime();
		kam.setOperateid(tkam.getOperateid());
		kam.setRouteType(tkam.getRouteType());
		kam.setOperatedate(currentTime);
		kam.setDelflag(new Long(0));
		Long contractid = getContractIdbyKam(kam.getKamid());
		if (tkam.getStatus() != null)
			kam.setStatus(tkam.getStatus());
		if (contractid != null) {
			TKamContract contract = get(TKamContract.class, contractid);
			contract.setKamname(kam.getKamnamecn());
			contract.setKamaddress(kam.getAddress());
			contract.setKampostcode(kam.getPostcode());
			contract.setBalancetype(kam.getBalancetype());
			contract.setExpditurelmt(kam.getExpditurelmt());
//			List<TPriDeptCM> agents = contractservice.getAllAgents((kam.getKamid()));
//			contractservice.doSaveAgent(agents, contract.getContractid());
			getEntityManager().update(contract);
		}
		// 如果客户中文名修改，对应的分公司名称也要修改
		// String oldnamecn = kam.getKamnamecn();
		// String newnamecn = tkam.getKamnamecn();
		// if(!oldnamecn.equals(newnamecn)){
		// String sql = "select t.companyid from t_kam_cmp_map t where
		// t.kamid="+tkam.getKamid();
		// Long companyid = (Long)getEntityManager().queryForObjectBySql(sql);
		// BeanQueryCondition bqc =
		// BeanQueryCondition.forClass(TKamCompany.class);
		// bqc.addExpressions(Restrictions.equal("companyid", companyid));
		// TKamCompany tkamCompany = (TKamCompany)getEntityManager().query(bqc);
		// tkamCompany.setCompanyname(newnamecn);
		// getEntityManager().saveOrUpdate(tkamCompany);
		// }
		if (kam != null) {
			if (tkam.getKamno().length() == 6) {
				int temp = Integer.parseInt(tkam.getKamno()) % 6;// 客户编号除以6的余数
				String tkamno = tkam.getKamno() + String.valueOf(temp);// 把余数补到最后
				kam.setKamno(tkamno);// 设置客户编号
			}
			getEntityManager().update(kam);
			// 更新奖励政策之前，删除已经的奖励政策
			List<Long> pers = new ArrayList<Long>();
			if (perbonusid != null) {
				for (int i = 0; i < perbonusid.length; i++) {
					pers.add(new Long(perbonusid[i].trim()));
				}
			}
			List<Long> posts = new ArrayList<Long>();
			if (postbonusid != null) {
				for (int i = 0; i < postbonusid.length; i++) {
					posts.add(new Long(postbonusid[i].trim()));
				}
			}
			//added by hchang 删除掉所有的奖励政策关联
//			String sqldel="delete T_KAM_BASISPOLICY_MAP where kamid="+tkam.getKamid();
//			getEntityManager().executeUpdateBySql(sqldel);
			//added by hchang 将现有合同的有效性都变无效
//			String sqlupdate="update   T_KAM_CONTRACT t set t.CONTRACTSTATUS=1  where  t.kamid="+tkam.getKamid();
//			getEntityManager().executeUpdateBySql(sqldel);
//			contractservice.doSelectPolicyByKamId(pers, posts, tkam.getKamid(),null);
			// 更新代理人
			List<Long> agent = new ArrayList<Long>();
			if (departmentid != null) {
				for (int i = 0; i < departmentid.length; i++) {
					agent.add(new Long(departmentid[i].trim()));	
				}
				//过虑重复的id号
				for(int t=0;t<agent.size();t++){
					String sub1	= agent.get(t).toString();
					for(int r=t+1;r<agent.size();r++){
					  String sub2 = agent.get(r).toString();
						if(sub1.equals(sub2)){
							agent.remove(r);				
						}						
					}						
				}
			    //如果是两方，那么代理人默认加iataCode='08673556'的代理人
				if(tkam.getAgreementtype() != null && tkam.getAgreementtype()== 0){
					List templist = new ArrayList();
					String sql = "select departmentid from t_info_iata t where t.IATACODE='08673556' and t.departmentid is not null";
					templist = getEntityManager().queryForListBySql(sql);
					if(templist!=null && templist.size()>0){
						Long id = Long.valueOf(templist.get(0).toString());				
							if(!agent.contains(id)){
								agent.add(id);					
						}					
					}
					List temp2list = new ArrayList();
					String sql2 = "select departmentid from t_info_iata t where t.IATACODE='08677917' and t.departmentid is not null";
					temp2list = getEntityManager().queryForListBySql(sql2);
					if(temp2list!=null && temp2list.size()>0){
						Long id = Long.valueOf(temp2list.get(0).toString());				
							if(!agent.contains(id)){
								agent.add(id);					
						}					
					}
				}
			}else{
				if(tkam.getAgreementtype() != null && tkam.getAgreementtype()== 0 ){
					List templist = new ArrayList();
					String sql = "select departmentid from t_info_iata t where t.IATACODE='08673556' and t.departmentid is not null";
					templist = getEntityManager().queryForListBySql(sql);
					if(templist!=null && templist.size()>0){
						Long id = Long.valueOf(templist.get(0).toString());				
							if(!agent.contains(id)){
								agent.add(id);					
						}					
					}
					List temp2list = new ArrayList();
					@SuppressWarnings("unused")
					String sql2 = "select departmentid from t_info_iata t where t.IATACODE='08677917' and t.departmentid is not null";
					temp2list = getEntityManager().queryForListBySql(sql);
					if(temp2list!=null && temp2list.size()>0){
						Long id = Long.valueOf(templist.get(0).toString());				
							if(!agent.contains(id)){
								agent.add(id);					
						}					
					}
				}
			}
			try {
				this.updateAgent(agent, tkam.getKamid());
			} catch (Exception e) {
				e.printStackTrace();
			}
			// 更新代理人联系人
			List<Long> contact = new ArrayList<Long>();
			if (contactid != null) {
				for (int i = 0; i < contactid.length; i++) {
					contact.add(new Long(contactid[i].trim()));
				}
				//过虑重复的id号
				for(int t=0;t<contact.size();t++){
					String sub1	= contact.get(t).toString();
					for(int r=t+1;r<contact.size();r++){
					  String sub2 = contact.get(r).toString();
						if(sub1.equals(sub2)){
							contact.remove(r);				
						}						
					}						
				}
			}
			try {
				this.updateContact(contact, tkam.getKamid());

			} catch (Exception e) {
				e.printStackTrace();
			}
			// 更新主营业务
			List<Long> biz = new ArrayList<Long>();
			if (biztype != null && biztype.length > 0) {
				for (int i = 0; i < biztype.length; i++) {
					biz.add(new Long(biztype[i]));
				}
			}
			try {
				this.updateBizType(biz, tkam.getKamid());

			} catch (Exception e) {
				e.printStackTrace();
			}
			//更新 特殊航线备注
			if(specialAirline != null){
				for (int i = 0; i < specialAirline.size(); i++) {
					TSpecialAirline sAirline = specialAirline.get(i);
					List<TSpaceDiscount> spaceDiscounts = sAirline.getTSpaceDiscounts();
					for (int j = 0; j < spaceDiscounts.size(); j++) {
						if("on".equals(spaceDiscounts.get(j).getSpaceType())){
							spaceDiscounts.get(j).setSpaceType("true");
						}
					}
				}
				this.updateSpecialAirline(specialAirline,tkam.getKamid());
			}
			
			//更新增值服务
			List<Long> service = new ArrayList<Long>();
			if (serviceid != null && serviceid.length > 0) {
				for (int i = 0; i < serviceid.length; i++) {
					service.add(new Long(serviceid[i]));
				}
			}
			try {
				this.updateService(service, tkam.getKamid());

			} catch (Exception e) {
				e.printStackTrace();
			}
			// 更新关键人信息
//			BeanQueryCondition mapbqc = BeanQueryCondition.forClass(TKamCntlMap.class);
//			mapbqc.addExpressions(Restrictions.equal("TKam.kamid", tkam.getKamid()));
//			mapbqc.addExpressions(Restrictions.equal("TKamCustomer.customertype", 0l));
//			List<TKamCntlMap> cntmap = getEntityManager().query(mapbqc);
//			List<TKamCustomer> cusidlist = new ArrayList();
//			Iterator it = cntmap.iterator();
//			while (it.hasNext()) {
//				TKamCntlMap cntl = (TKamCntlMap) it.next();
//				cusidlist.add(cntl.getTKamCustomer());
//			}
//			getEntityManager().delete(cntmap);
//			getEntityManager().delete(cusidlist);
//			if (tkamImportant != null && tkamImportant.size() > 0) {
//				for (TKamCustomer importantVO : tkamImportant) {
//					TKamCustomer vo = new TKamCustomer();
//
//					vo.setCustomertype(new Long(0));
//					if (importantVO.getNamecn() == null || "".equals(importantVO.getNamecn())) {
//						vo.setNamecn("无");
//					} else {
//						vo.setNamecn(importantVO.getNamecn());
//					}
//					vo.setDelflag(new Long(0));
//					vo.setCustomertype(0l);
//					vo.setEmail(importantVO.getEmail());
//					vo.setIdcardno(importantVO.getIdcardno());
//					vo.setPosition(importantVO.getPosition());
//					vo.setPassportcode(importantVO.getPassportcode());
//					vo.setTelephone(importantVO.getTelephone());
//					vo.setTelephone2(importantVO.getTelephone2());
//					vo.setOthercode(importantVO.getOthercode());
//					vo.setFirstnamecn(importantVO.getFirstnamecn());
//					vo.setLastnamecn(importantVO.getLastnamecn());
//					vo.setNamecn(importantVO.getLastnamecn() + importantVO.getFirstnamecn());	
//					getEntityManager().save(vo);
//
//					Long customerid = vo.getCustomerid();
//					TKamCntlMap cntlmap = new TKamCntlMap();
//					TKamCntlMapId cntid = new TKamCntlMapId();
//					cntid.setTKam(getEntityManager().get(TKam.class,tkam.getKamid()));
//					cntid.setTKamCustomer(getEntityManager().get(TKamCustomer.class, customerid));
//					cntlmap.setId(cntid);
//					getEntityManager().save(cntlmap);
//					//调用WebService更新关键人信息
//					this.doUpdateCustomerToCC(tkam, vo);
//				}
//			}
//			// 更新联系人信息
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamContact.class);
			bqc.addExpressions(Restrictions.equal("TKam.kamid", tkam.getKamid()));
			List<TKamContact> oldContactList = getEntityManager().query(bqc);
			getEntityManager().delete(oldContactList);
			// 新增大客户对应的联系人，王永明2010-10-15
//			BeanQueryCondition mapbqc2 = BeanQueryCondition.forClass(TKamCntlMap.class);
//			mapbqc2.addExpressions(Restrictions.equal("TKam.kamid", tkam.getKamid()));
//			mapbqc2.addExpressions(Restrictions.equal(
//					"TKamCustomer.customertype", 1l));
//			List<TKamCntlMap> cntmap2 = getEntityManager().query(mapbqc2);
//			List<TKamCustomer> cusidlist2 = new ArrayList();
//			Iterator it2 = cntmap2.iterator();
//			while (it2.hasNext()) {
//				TKamCntlMap cntl = (TKamCntlMap) it2.next();
//				cusidlist2.add(cntl.getTKamCustomer());
//			}
//			getEntityManager().delete(cntmap2);
//			getEntityManager().delete(cusidlist2);
			
			if (tkamContact != null && tkamContact.size() > 0) {
				for (TKamCustomer contactVO : tkamContact) {
					TKamContact contactVo = new TKamContact();
					contactVo.setCreateDate(currentTime);        //创建日期
					contactVo.setCreatorid(tkam.getCreatorid()); //创建人ID
					contactVo.setOperatorid(tkam.getOperateid());//操作人ID
					contactVo.setOperateDate(currentTime);       //操作日期
					contactVo.setNamecn(contactVO.getNamecn()); //中文姓名
					contactVo.setFirstnamecn(contactVO.getFirstnamecn());    //中文姓
					contactVo.setLastnamecn(contactVO.getLastnamecn());      //中文名
					contactVo.setDelflag(new Long(0));                       //删除标记，默认为0
					contactVo.setFax(contactVO.getOthercode());                    //传真/其他
					contactVo.setTelephone(contactVO.getTelephone());        //电话
					contactVo.setMobile(contactVO.getMobile());              //手机
					contactVo.setEmail(contactVO.getEmail());                //电子邮件
					contactVo.setPosition(contactVO.getPosition());          //职位
					contactVo.setIdcardno(contactVO.getIdcardno());          //身份证
					contactVo.setPassportcode(contactVO.getPassportcode());
					contactVo.setTKam(tkam);
					
					getEntityManager().save(contactVo);
					
					//----------原有代码  开始------------------//
//					TKamCustomer vo = new TKamCustomer();
//					vo.setCustomertype(new Long(1));
//					if (contactVO.getNamecn() == null || "".equals(contactVO.getNamecn())) {
//						vo.setNamecn("无");
//					} else {
//						vo.setNamecn(contactVO.getNamecn());
//					}
//					vo.setDelflag(new Long(0));
//					vo.setCustomertype(1l);
//					vo.setEmail(contactVO.getEmail());
//					vo.setIdcardno(contactVO.getIdcardno());
//					vo.setPosition(contactVO.getPosition());
//					vo.setPassportcode(contactVO.getPassportcode());
//					vo.setTelephone(contactVO.getTelephone());
//					vo.setTelephone2(contactVO.getTelephone2());
//					vo.setMobile(contactVO.getMobile());
//					vo.setOthercode(contactVO.getOthercode());
//					vo.setFirstnamecn(contactVO.getFirstnamecn());
//					vo.setLastnamecn(contactVO.getLastnamecn());
//					vo.setNamecn(contactVO.getFirstnamecn() + contactVO.getLastnamecn());	
//					getEntityManager().save(vo);
//
//					Long customerid = vo.getCustomerid();
//					TKamCntlMap cntlmap = new TKamCntlMap();
//					TKamCntlMapId cntid = new TKamCntlMapId();
//					cntid.setTKam(getEntityManager().get(TKam.class, tkam.getKamid()));
//					cntid.setTKamCustomer(getEntityManager().get(TKamCustomer.class, customerid));
//					cntlmap.setId(cntid);
//					getEntityManager().save(cntlmap);
//					//调用WebService往CC中插关键人及中间表数据
//					this.doInsertCustomerToCC(tkam, vo);
				}
			}
			
//			if (tkamContact != null && tkamContact.size() > 0) {
//				for (TKamCustomer contactVO : tkamContact) {
//					TKamCustomer vo = new TKamCustomer();
//
//					vo.setCustomertype(new Long(1));
//					if (contactVO.getNamecn() == null || contactVO.getNamecn().equals("")) {
//						vo.setNamecn("无");
//					} else {
//						vo.setNamecn(contactVO.getNamecn());
//					}
//					vo.setDelflag(new Long(0));
//					vo.setCustomertype(1l);
//					vo.setEmail(contactVO.getEmail());
//					vo.setIdcardno(contactVO.getIdcardno());
//					vo.setPosition(contactVO.getPosition());
//					vo.setPassportcode(contactVO.getPassportcode());
//					vo.setTelephone(contactVO.getTelephone());
//					vo.setTelephone2(contactVO.getTelephone2());
//					vo.setOthercode(contactVO.getOthercode());
//					vo.setFirstnamecn(contactVO.getFirstnamecn());
//					vo.setLastnamecn(contactVO.getLastnamecn());
//					vo.setNamecn(contactVO.getLastnamecn() + contactVO.getFirstnamecn());	
//					getEntityManager().save(vo);
//
//					Long customerid = vo.getCustomerid();
//					TKamCntlMap cntlmap = new TKamCntlMap();
//					TKamCntlMapId cntid = new TKamCntlMapId();
//					cntid.setTKam(getEntityManager().get(TKam.class,
//							tkam.getKamid()));
//					cntid.setTKamCustomer(getEntityManager().get(
//							TKamCustomer.class, customerid));
//					cntlmap.setId(cntid);
//					getEntityManager().save(cntlmap);
//					//调用WebService更新关键人信息
//					this.doUpdateCustomerToCC(tkam, vo);
//
//				}
//			}
			// 更新营业部
			Long companyid = getCompanyidByKamid(kam.getKamid());
			if (tkam.getTPriDept().getDepartmentid() != null) {
				if(!"approved".equals(isGroupId)) {
					distributeService.doDistributeDeptWithOutGroup(kam.getKamid(), companyid,
						rolecode, usercode, tkam.getTPriDept(), false);
				}else {//全功能
					distributeService.doDistributeDeptWithOutGroupQuan(kam.getKamid(), companyid,
							rolecode, usercode, tkam.getTPriDept(), false);
				}
			}
			// 纪录操作日志
			TKamLog log = new TKamLog();
			log.setTKam(tkam);
			tkam = get(TKam.class, tkam.getKamid());
			log.setOperatorid(tkam.getOperateid());
			log.setOperatedate(currentTime);
			if (tkam.getStatus() == 13L || tkam.getStatus() == 14L) {
				log.setOperatetype(4l);
			} else {
				log.setOperatetype(3L);
			}
			log.setRemark(tkamLog.getRemark());
			getEntityManager().save(log);
			
			//调用WebService更新关键人信息
			doSyncKamTocc(tkam.getKamno());
			/*Boolean logtemp =this.doUpdatetTKamToCC(tkam,specialAirline);
			//调用成功纪录日志
			if(logtemp==true){
				TKamLog logsucess = new TKamLog();
				logsucess.setTKam(tkam);
				logsucess.setOperatorid(tkam.getOperateid());
				logsucess.setOperatedate(currentTime);
				logsucess.setOperatetype(new Long(11));
				logsucess.setRemark("同步callcenter成功");
				getEntityManager().save(logsucess);	
			}
			//调用失败纪录日志
			else{
				TKamLog logfaild = new TKamLog();
				logfaild.setTKam(tkam);
				logfaild.setOperatorid(tkam.getOperateid());
				logfaild.setOperatedate(currentTime);
				logfaild.setOperatetype(new Long(10));
				logfaild.setRemark("同步callcenter失败");
				getEntityManager().save(logfaild);			
			}*/
			// 同步更新B2B数据
			/*
			 * StringBuffer hql = new StringBuffer(); String kamno
			 * =tkam.getKamno(); hql.append("from TrCompVO t where
			 * t.kamno='"+kamno + "'"); TrCompVO compVO =
			 * (TrCompVO)getB2bEntityManager().queryForObjectByHql(hql.toString());
			 * compVO.setKamno(tkam.getKamno());
			 * compVO.setCompNm(tkam.getKamnamecn());
			 * compVO.setCompEnm(tkam.getKamnameen());
			 * this.getB2bEntityManager().save(compVO);
			 */
		}
	}

	// 取得companyid
	@SuppressWarnings("unchecked")
	public Long getCompanyidByKamid(Long kamid) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamCmpMap.class);
		bqc.addExpressions(Restrictions.equal("kamid", kamid));
		List result = getEntityManager().query(bqc);
		Long companyid = ((TKamCmpMap) result.get(0)).getTKamCompany()
				.getCompanyid();
		return companyid;
	}

	// 取得大客户对应的客服代表即客户经理
	@SuppressWarnings("unchecked")
	public List<TPriAccount> queryAccount(Long kamid) throws ServiceException {
		List<Object> list = this.queryTKamCmpMap(kamid);
		List<Long> newlist = new ArrayList<Long>();
		BeanQueryCondition bqc1 = BeanQueryCondition.forClass(TKamCompany.class);
		for (Object id : list) {
			newlist.add(Long.parseLong(((BigDecimal) id).toString()));
			if(list.indexOf(id)>10) break;
		}
		if (newlist != null && newlist.size() > 0) {
			bqc1.addExpressions(Restrictions.in("companyid", newlist));
			List accountidlist = getEntityManager().query(bqc1);// TKamCompany表中的AccountId
			if (accountidlist != null && accountidlist.size() > 0) {
				List newlist2 = new ArrayList();
				Iterator iterator = accountidlist.iterator();
				while (iterator.hasNext()) {
					Long accountid = ((TKamCompany) iterator.next())
							.getAccountid();
					if (accountid != null) {
						newlist2.add(accountid);
					}
				}
				if (newlist2 != null && newlist2.size() > 0) {
					BeanQueryCondition bqc2 = BeanQueryCondition
							.forClass(TPriAccount.class);
					bqc2.addExpressions(Restrictions.in("accountid", newlist2));
					List<TPriAccount> tpriAccount = getEntityManager().query(
							bqc2);
					return tpriAccount;
				}
			}
		}
		return null;
	}
	/**
	 * 	根据kamid取得大客户的联系人
	 */
	@SuppressWarnings("unchecked")
	public List<TKamContact> queryContactByKamId(Long kamid) throws ServiceException {
		//=========从t_kam_contact表中取联系人信息   开始===========//
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamContact.class);
		bqc.addExpressions(Restrictions.equal("kamid", Long.valueOf(kamid)));
		bqc.addExpressions(Restrictions.equal("delflag", Long.valueOf(0)));
		List<TKamContact> contactlist = getEntityManager().query(bqc);
		
		//=========从t_kam_contact表中取联系人信息   结束===========//
		return contactlist;
	}
	
	// 取得大客户的联系人&关键人
	@SuppressWarnings("unchecked")
	public List<TKamCustomer> queryCustomerByKamId(Long kamid, Long customertype)
			throws ServiceException {
		List<TKamCustomer> customerlist = new ArrayList<TKamCustomer>();
		//customertype为0，表示关键人，从T_KAM_CUSTOMER中取数据
		if(customertype == 0){
			List<Object> list = this.queryTKamCntlMap(kamid);
			List<Long> newlist = new ArrayList<Long>();
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamCustomer.class);
	
			for (int i = 0; i < list.size(); i++) {
				if(i<10) newlist.add(Long.parseLong(((BigDecimal) list.get(i)).toString()));
			}
			if (newlist != null && newlist.size() > 0) {
				bqc.addExpressions(Restrictions.in("customerid", newlist));
				bqc.addExpressions(Restrictions.equal("delflag", Long.valueOf(0)));
				bqc.addExpressions(Restrictions.equal("customertype",customertype));
				customerlist = getEntityManager().query(bqc);
				
			}
		//customertype为1，表示联系人，从表T_KAM_CONTACT中取数据
		}else{
			List<TKamContact> contactlist = new ArrayList<TKamContact>();
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamContact.class);
			bqc.addExpressions(Restrictions.equal("kamid", kamid));
			contactlist = getEntityManager().query(bqc);
			
			if(contactlist != null && contactlist.size()>0){
				for(TKamContact contact : contactlist){
					TKamCustomer customer = new TKamCustomer();
					customer.setFirstnamecn(contact.getFirstnamecn());
					customer.setLastnamecn(contact.getLastnamecn());
					customer.setNamecn(contact.getNamecn());
					customer.setEmail(contact.getEmail());
					customer.setIdcardno(contact.getIdcardno());
					customer.setPosition(contact.getPosition());
					customer.setPassportcode(contact.getPassportcode());
					customer.setTelephone(contact.getTelephone());
					customer.setMobile(contact.getMobile());
					customer.setOthercode(contact.getFax());
					customerlist.add(customer);
				}
			}
		}
		if(customerlist != null && customerlist.size()>0){
			return customerlist;
		}
		return null;
	}

	// 取得大客户对应的前返政策
	@SuppressWarnings("unchecked")
	public List<TBasispolicyPerbonusMap> queryPerbonusByKamId(Long kamid,
			Long perbonustype) throws ServiceException {
		TKamBasispolicyMap tkamBasispolicyMap = this.queryTKamBasispolicyMap(
				kamid, null);
		if (tkamBasispolicyMap != null) {
			Long basispolicyid = tkamBasispolicyMap.getId()
					.getTIncentiveBasispolicy().getBasispolicyid();
			if (basispolicyid != null) {
				BeanQueryCondition bqc1 = BeanQueryCondition
						.forClass(TBasispolicyPerbonusMap.class);
				bqc1.addExpressions(Restrictions.equal(
						"TIncentiveBasispolicy.basispolicyid", basispolicyid));
				bqc1.addExpressions(Restrictions.equal(
						"TIncentivePerbonus.perbonustype", perbonustype));
				bqc1.addInitFields("TIncentivePerbonus");
				bqc1.addInitFields("TIncentiveBasispolicy");
				List<TBasispolicyPerbonusMap> perbonuslist = getEntityManager()
						.query(bqc1);
				return perbonuslist;
			}
		}
		return null;
	}

	// 取得合同对应的前返政策
	@SuppressWarnings("unchecked")
	public List<TBasispolicyPerbonusMap> queryPerbonusByContractId(
			Long perbonustype, Long contractId) throws ServiceException {
		TKamBasispolicyMap tkamBasispolicyMap = this
				.queryTKamBasispolicyMapByContract(contractId);
		if (tkamBasispolicyMap != null) {
			Long basispolicyid = tkamBasispolicyMap.getId()
					.getTIncentiveBasispolicy().getBasispolicyid();
			if (basispolicyid != null) {
				BeanQueryCondition bqc1 = BeanQueryCondition
						.forClass(TBasispolicyPerbonusMap.class);
				bqc1.addExpressions(Restrictions.equal(
						"TIncentiveBasispolicy.basispolicyid", basispolicyid));
				bqc1.addExpressions(Restrictions.equal(
						"TIncentivePerbonus.perbonustype", perbonustype));
				bqc1.addInitFields("TIncentivePerbonus");
				bqc1.addInitFields("TIncentiveBasispolicy");
				List<TBasispolicyPerbonusMap> perbonuslist = getEntityManager()
						.query(bqc1);
				return perbonuslist;
			}
		}
		return null;
	}

	// 取得大客户对应的后返政策
	@SuppressWarnings("unchecked")
	public List<TPolicyPostbonusMap> queryPostbonusByKamId(Long kamid)
			throws ServiceException {
		TKamBasispolicyMap tkamBasispolicyMap = this.queryTKamBasispolicyMap(
				kamid, null);
		if (tkamBasispolicyMap != null) {
			Long basispolicyid = tkamBasispolicyMap.getId()
					.getTIncentiveBasispolicy().getBasispolicyid();
			if (basispolicyid != null) {
				BeanQueryCondition bqc1 = BeanQueryCondition
						.forClass(TPolicyPostbonusMap.class);
				bqc1.addExpressions(Restrictions.equal(
						"TIncentiveBasispolicy.basispolicyid", basispolicyid));
				bqc1.addInitFields("TIncentivePostbonus");
				bqc1.addInitFields("TIncentiveBasispolicy");
				List<TPolicyPostbonusMap> postbonuslist = getEntityManager()
						.query(bqc1);
				return postbonuslist;
			}
		}
		return null;
	}

	// 取得大客户对应的代理人
	@SuppressWarnings("unchecked")
	public List<TPriDeptCM> queryAgentByKam(Long kamid) throws ServiceException {
		String sql = "select t.departmentid from t_kam_agent_map t where t.kamid=' "
				+ kamid + "'";
		List<Object> tpriDeptCM = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> list = new ArrayList<Long>();
		for (Object id : tpriDeptCM) {
			list.add(Long.parseLong(((BigDecimal) id).toString()));
		}
		if (list != null && list.size() > 0) {
			BeanQueryCondition bqc1 = BeanQueryCondition
					.forClass(TPriDeptCM.class);
			bqc1.addExpressions(Restrictions.in("ME_departmentid", list));
			List<TPriDeptCM> tpriDeptCMList = getEntityManager().query(bqc1);
			return tpriDeptCMList;
		}
		return null;
	}

	// 取得大客户对应的联系人
	@SuppressWarnings("unchecked")
	public List<TPriContact> queryContactByKamImage(Long kamid)
			throws ServiceException {
		String sql = "select t.contactid from t_kam_contact_map t where t.imageid=' "
				+ kamid + "'";
		List<Object> tpricontact = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> list = new ArrayList<Long>();
		for (Object id : tpricontact) {
			list.add(Long.parseLong(((BigDecimal) id).toString()));
		}
		if (list != null && list.size() > 0) {
			BeanQueryCondition bqc1 = BeanQueryCondition
					.forClass(TPriContact.class);
			bqc1.addExpressions(Restrictions.in("contactid", list));
			bqc1.addInitFields("TPriDept");
			List<TPriContact> TPriContactList = getEntityManager().query(bqc1);
			return TPriContactList;
		}
		return null;
	}

	// 取得大客户对应的联系人
	@SuppressWarnings("unchecked")
	public List<TPriContact> queryContactByKam(Long kamid)
			throws ServiceException {
		String sql = "select t.contactid from t_kam_contact_map t where t.kamid=' "
				+ kamid + "'";
		List<Object> tpricontact = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> list = new ArrayList<Long>();
		for (Object id : tpricontact) {
			list.add(Long.parseLong(((BigDecimal) id).toString()));
		}
		if (list != null && list.size() > 0) {
			BeanQueryCondition bqc1 = BeanQueryCondition
					.forClass(TPriContact.class);
			bqc1.addExpressions(Restrictions.in("contactid", list));
			bqc1.addInitFields("TPriDept");
			List<TPriContact> TPriContactList = getEntityManager().query(bqc1);
			return TPriContactList;
		}
		return null;
	}

	// 取得合同对应的后返政策
	@SuppressWarnings("unchecked")
	public List<TPolicyPostbonusMap> queryPostbonusByContract(Long contractid)
			throws ServiceException {
		TKamBasispolicyMap tkamBasispolicyMap = this
				.queryTKamBasispolicyMapByContract(contractid);
		if (tkamBasispolicyMap != null) {
			Long basispolicyid = tkamBasispolicyMap.getId()
					.getTIncentiveBasispolicy().getBasispolicyid();
			if (basispolicyid != null) {
				BeanQueryCondition bqc1 = BeanQueryCondition
						.forClass(TPolicyPostbonusMap.class);
				bqc1.addExpressions(Restrictions.equal(
						"TIncentiveBasispolicy.basispolicyid", basispolicyid));
				bqc1.addInitFields("TIncentivePostbonus");
				bqc1.addInitFields("TIncentiveBasispolicy");
				List<TPolicyPostbonusMap> postbonuslist = getEntityManager()
						.query(bqc1);
				return postbonuslist;
			}
		}
		return null;
	}

	// 查询操作日志
	@SuppressWarnings("unchecked")
	public List<TKamLog> queryTKamLog(Long kamid) {
		String hql = "from TKamLog l where l.TKam.kamid=" + kamid
				+ " and l.operatetype!='11'"
				+ " and l.operatetype!='10'"
				+ " and l.operatetype!='7'"
				+ " and l.operatetype!='8'"
				+ " order by l.operatedate desc";
		return getEntityManager().queryForListByHql(hql);
	}

	// 查询Image操作日志
	@SuppressWarnings("unchecked")
	public List<TKamLog> queryTKamImageLog(Long kamid) {
		String hql = "from TKamLog l where l.imageid=" + kamid
				+ " order by l.operatedate desc";
		return getEntityManager().queryForListByHql(hql);
	}

	// 查询TKam-TKamCustomer中间表
	@SuppressWarnings("unchecked")
	public List<Object> queryTKamCmpMap(Long kamid) throws ServiceException {
		String sql = "select t.companyId from t_kam_cmp_map t where t.kamId=' "
				+ kamid + "'";
		return getEntityManager().queryForListBySql(sql);
	}

	// 查询TKam-TKamCustomer中间表
	@SuppressWarnings("unchecked")
	public List<Object> queryTKamCntlMap(Long kamid) throws ServiceException {
		String sql = "select t.customerId from t_kam_cntl_map t where t.kamId=' "
				+ kamid + "'";
		return getEntityManager().queryForListBySql(sql);
	}

	// TIncentiveBasispolicy-TIncentivePerbonus中间表
	@SuppressWarnings("unchecked")
	public List<Object> queryTBasispolicyPerbonusMap(Long basispolicyid)
			throws ServiceException {
		String sql = "select t.perbonusId from t_basispolicy_perbonus_map t where t.basispolicyId=' "
				+ basispolicyid + "'";
		return getEntityManager().queryForListBySql(sql);
	}

	// 查询TIncentiveBasispolicy-TIncentivePostbonus中间表
	@SuppressWarnings("unchecked")
	public List<Object> queryTBasispolicyPostbonusMap(Long basispolicyid)
			throws ServiceException {
		String sql = "select t.postbonusId from t_policy_postbonus_map t where t.basispolicyId=' "
				+ basispolicyid + "'";
		return getEntityManager().queryForListBySql(sql);
	}

	// 根据ID取得大客户信息
	@SuppressWarnings("unchecked")
	public TKam getTKamById(Long kamid) {
		return getEntityManager().get(TKam.class, kamid, "TPriDept.deptname");
	}

	// B2B数据库comCd加一补为5为数
	@SuppressWarnings("unchecked")
	public String haoAddOne(String copmcd) {
		Integer intHao = Integer.parseInt(copmcd);
		intHao++;
		DecimalFormat df = new DecimalFormat(STR_FORMAT);
		return df.format(intHao);
	}

	// 查询主营业务
	@SuppressWarnings("unchecked")
	public List<TKamBiztype> queryTKamBiztype(Long kamid) {
		String sql = "select t.biztypeid from t_kam_biz_map t where t.kamid='"
				+ kamid + "'";
		List<Object> bizlist = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> list = new ArrayList<Long>();
		if (bizlist != null && bizlist.size() > 0) {	
			for (Object id : bizlist) {
				list.add(Long.parseLong(((BigDecimal) id).toString()));
			}
		}
		if (list != null && list.size() > 0) {
			BeanQueryCondition bqc = BeanQueryCondition
					.forClass(TKamBiztype.class);
			bqc.addExpressions(Restrictions.in("biztypeid", list));
			List<TKamBiztype> biztypelist = getEntityManager().query(bqc);
			return biztypelist;
		}
		return null;
	}

	// 取得营业部弹出框所需部门数据
	@SuppressWarnings("unchecked")
	public List<TPriDeptCM> getDept() {
		String hql = "from TPriDeptCM dept where dept.ME_delflag=0 and dept.ME_depttype=0"
				+ " and exists ( select 'x' from TPriGroupCM group where group.ME_grouplevel=0 and group.ME_deptid=dept.ME_departmentid)";
		List<TPriDeptCM> list = getEntityManager().queryForListByHql(hql);
		for (int i = 0; i < list.size(); i++) {
			Long groupId = ((TPriGroupCM) getEntityManager().queryForListByHql(
					"from TPriGroupCM group where group.ME_grouplevel=0 and group.ME_deptid="
							+ list.get(i).getME_departmentid()).get(0))
					.getME_groupid();
			list.get(i).setGroupId(groupId);
		}
		return list;
	}

	// 查询TKam-TIncentiveBasispolicy中间表
	@SuppressWarnings("unchecked")
	private TKamBasispolicyMap queryTKamBasispolicyMapByContract(Long contractid) {
		String hql1 = "from TKamBasispolicyMap map where map.contractid="
				+ contractid;
		List<TKamBasispolicyMap> list2 = getEntityManager().queryForListByHql(
				hql1);
		if (list2.size() != 0)
			return list2.get(0);
		return null;
	}

	// 确定由续签合同，并返回续签合同的奖励政策
	@SuppressWarnings("unchecked")
	public TKamBasispolicyMap getReNewPolicy(Long kamid) {
		String hql = "from TKamBasispolicyMap map where map.id.TKam.kamid="
				+ kamid + " and contractid is not null";
		List<TKamBasispolicyMap> list2 = getEntityManager().queryForListByHql(
				hql);
		if (list2.size() == 0)
			return null;
		else {
			hql = "from TKamContract contract where  contract.isagreement<>1 and contract.TKam.kamid="
					+ kamid
					+ " and contract.delflag=0 and contract.languagetype=3 order by contract.operatedate desc";
			List<TKamContract> list = getEntityManager().queryForListByHql(hql);
			if (list.size() != 0) {
				TKamContract c = list.get(0);
				hql = "from TKamBasispolicyMap map where map.id.TKam.kamid="
						+ kamid + " and map.contractid=" + c.getContractid();
				list2 = getEntityManager().queryForListByHql(hql);
				if (list2.size() != 0)
					return list2.get(0);
			}
		}
		return null;
	}

	// 查找要联动的合同
	@SuppressWarnings("unchecked")
	public Long getContractIdbyKam(Long kamid) {
		String hql = "from TKamContract c where c.delflag=0 and  c.TKam.kamid="
				+ kamid;
		List l = getEntityManager().queryForListByHql(hql);
		TKamBasispolicyMap map = null;
		if (l.size() != 0)
			map = queryTKamBasispolicyMap(kamid, true);
		else
			map = queryTKamBasispolicyMap(kamid, false);
		if (map != null)
			return map.getContractid();
		else
			return null;
	}

	// 查询TKam-TIncentiveBasispolicy中间表,若 hasContract＝true,排除无合同的id的中间表
	@SuppressWarnings("unchecked")
	public TKamBasispolicyMap queryTKamBasispolicyMap(Long kamid,
			Boolean hasContract) {
		TKamBasispolicyMap aaa = getReNewPolicy(kamid);
		if (aaa != null)
			return aaa;
		if (hasContract == null || (hasContract != null && !hasContract)) {
			String hql1 = "from TKamBasispolicyMap map where map.id.TKam.kamid="
					+ kamid + " and contractid is null";
			List<TKamBasispolicyMap> list2 = getEntityManager()
					.queryForListByHql(hql1);
			if (list2.size() != 0)
				return list2.get(0);
		}
		String hql = "from TKamContract contract where contract.contractstatus=0 and contract.isagreement<>1 and contract.TKam.kamid="
				+ kamid + " and contract.delflag=0";
		List<TKamContract> list = getEntityManager().queryForListByHql(hql);
		if (list.size() == 0) {
			hql = "from TKamContract contract where contract.TKam.kamid="
					+ kamid
					+ " and contract.isagreement<>1 and contract.delflag=0";
			list = getEntityManager().queryForListByHql(hql);
			if (list.size() == 0)
				return null;
			TKamContract contract = list.get(0);
			hql = "from TKamBasispolicyMap map where map.id.TKam.kamid="
					+ kamid + " and map.contractid=" + contract.getContractid();
			List<TKamBasispolicyMap> kamBasispolicyMap = getEntityManager()
					.queryForListByHql(hql);
			if (kamBasispolicyMap != null && kamBasispolicyMap.size() > 0) {
				return (TKamBasispolicyMap) getEntityManager()
						.queryForListByHql(hql).get(0);
			}
			return null;
		} else {
			@SuppressWarnings("unused")
			TKamContract contract = list.get(0);
			hql = "from TKamBasispolicyMap map where map.id.TKam.kamid="+ kamid ;
			//+ " and map.contractid=" + contract.getContractid();
			try {
				return (TKamBasispolicyMap) getEntityManager()
						.queryForListByHql(hql).get(0);
			} catch (Exception e) {
				return null;
			}
		}
	}

	// 取得代理人弹出框所需代理人数据
	@SuppressWarnings("unchecked")
	public List<TPriDept> getAgent(AgentDto agentDto, OrderablePagination p) {

		BeanQueryCondition bqc = BeanQueryCondition.forClass(TPriDept.class);
		String parentDept = agentDto.getName();
		if (StringUtils.hasText(parentDept)) {
			String sql1 = "select t.departmentId from T_PRI_DEPT t where t.DEPTTYPE=0 and t.DEPTNAME like '%"
					+ parentDept + "%'";
			List<Object> list1 = getEntityManager().queryForListBySql(sql1);
			List<Integer> newlist1 = new ArrayList<Integer>();
			for (Object id : list1) {
				newlist1.add(Integer.valueOf((((BigDecimal) id).toString())));
			}
			if (newlist1 != null && newlist1.size() > 0) {
				bqc.addExpressions(Restrictions.in("parentDeptId", newlist1));
			} else {
				return new ArrayList();
			}
		}
		String iata = agentDto.getIatacode();
		if (StringUtils.hasText(iata)) {
			String sql2 = "select t.departmentid from t_info_iata t where t.iatacode='"
					+ iata + "'";
			List<Object> list2 = getEntityManager().queryForListBySql(sql2);
			List<Long> newlist2 = new ArrayList<Long>();
			if(list2!=null && list2.size()>0){
				for (int i = 0; i<list2.size(); i++) {
					if(list2.get(i)!=null){			
						newlist2.add(Long.valueOf((list2.get(i).toString())));
					}
				}				
			}
			if (newlist2 != null && newlist2.size() > 0) {
				bqc.addExpressions(Restrictions.in("departmentid", newlist2));
			} else {
				return new ArrayList();
			}
		}
		String office = agentDto.getOfficecode();
		if (StringUtils.hasText(office)) {
			String sql3 = "select t.departmentid from t_info_office t where t.officecode='"
					+ office + "'";
			List<Object> list3 = getEntityManager().queryForListBySql(sql3);
			List<Long> newlist3 = new ArrayList<Long>();
			for (Object id : list3) {
				newlist3.add(Long.valueOf(((((BigDecimal) id).toString()))));
			}
			if (newlist3 != null && newlist3.size() > 0) {
				bqc.addExpressions(Restrictions.in("departmentid", newlist3));
			} else {
				return new ArrayList();
			}
		}
		bqc.addExpressions(Restrictions.and(Restrictions.like("deptname",
				agentDto.getDeptname()), Restrictions.equal("depttype",
				new Long(1)), Restrictions.equal("delflag", new Long(0))));
		Sorter[] sorters = p.getSorters();
		if (ObjectUtils.isEmpty(sorters)) {
			bqc.addSorters(Sorter.desc("departmentid"));
		} else {
			bqc.addSorters(sorters);
		}

		List<TPriDept> result = getEntityManager().queryWithPagination(bqc, p);

		for (int i = 0; i < result.size(); i++) {
			Long deptid = result.get(i).getDepartmentid();
			String sql = "select t.iatacode from t_info_iata t where t.departmentid="
					+ deptid;
			List iatacode = getEntityManager().queryForListBySql(sql);
			if (null != iatacode && iatacode.size() > 0) {
				result.get(i).setIatacode(iatacode.get(0).toString());
			} else {
				// result.get(i).setIatacode("无对应的iatacode");
			}
		}
		for (int i = 0; i < result.size(); i++) {
			Long deptid = result.get(i).getDepartmentid();
			String sql = "select t.officecode from t_info_office t where t.departmentid="
					+ deptid;
			List officecode = getEntityManager().queryForListBySql(sql);
			if (null != officecode && officecode.size() > 0) {
				result.get(i).setOfficecode(officecode.get(0).toString());
			} else {
				// result.get(i).setOfficecode("无对应的officecode");
			}
		}
		return result;
	}

	// 删除大客户，delfg设置为1
	public void doDeleteKam(Long kamid) {
		TKam kam = getEntityManager().get(TKam.class, kamid);
		if (kam != null) {
			kam.setDelflag(new Long(1));
		}
	}

	// 查询TkamBasispolicyMap中间表,确实大客户是否有对应的合同(ContractID)
	@SuppressWarnings("unchecked")
	public List<Long> getContractIDList(Long kamid) {
		String sql = "select t.contractid from t_kam_contract t where t.delflag=0 and t.ISAGREEMENT<>1 and  t.kamid ="
				+ kamid;
		List<Long> ContractIDList = getEntityManager().queryForListBySql(sql);
		return ContractIDList;
	}

	// 保存代理人
	@SuppressWarnings("unchecked")
	public void saveAgent(List<Long> agent, Long kamid) throws Exception {
		for (int j = 0; j < agent.size(); j++) {
			TKamAgentMap tkamAgentMap = new TKamAgentMap();
			TKamAgentMapId tkamAgentMapId = new TKamAgentMapId();
			tkamAgentMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamAgentMapId.setTPriDept(getEntityManager().get(TPriDept.class,
					agent.get(j)));
			tkamAgentMap.setId(tkamAgentMapId);
			doSave(tkamAgentMap);
		}
	}
	// 保存代理人联系人
	@SuppressWarnings("unchecked")
	public void saveContact(List<Long> contact, Long kamid) throws Exception {
		for (int j = 0; j < contact.size(); j++) {
			TKamContactMap tkamContactMap = new TKamContactMap();
			TKamContactMapId tkamContactMapId = new TKamContactMapId();
			tkamContactMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamContactMapId.setTPriContact(getEntityManager().get(
					TPriContact.class, contact.get(j)));
			tkamContactMap.setId(tkamContactMapId);
			doSave(tkamContactMap);
		}
	}
	// 更新代理人
	@SuppressWarnings("unchecked")
	public void updateAgent(List<Long> agent, Long kamid) throws Exception {
		String hql = "from TKamAgentMap map where map.id.TKam.kamid=" + kamid;
		List<TKamAgentMap> agentmap = getEntityManager().queryForListByHql(hql);
		if (agentmap.size() > 0) {
			getEntityManager().delete(agentmap);
		}
		for (int j = 0; j < agent.size(); j++) {
			TKamAgentMap tkamAgentMap = new TKamAgentMap();
			TKamAgentMapId tkamAgentMapId = new TKamAgentMapId();
			tkamAgentMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamAgentMapId.setTPriDept(getEntityManager().get(TPriDept.class,
					agent.get(j)));
			tkamAgentMap.setId(tkamAgentMapId);
			doSave(tkamAgentMap);
		}
	}
	// 更新代理人联系人
	@SuppressWarnings("unchecked")
	public void updateContact(List<Long> contact, Long kamid) throws Exception {
		String hql = "from TKamContactMap map where map.id.TKam.kamid=" + kamid;
		List<TKamContactMap> contactmap = getEntityManager().queryForListByHql(
				hql);
		if (contactmap.size() > 0) {
			getEntityManager().delete(contactmap);
		}
		for (int j = 0; j < contact.size(); j++) {
			TKamContactMap tkamContactMap = new TKamContactMap();
			TKamContactMapId tkamContactMapId = new TKamContactMapId();
			tkamContactMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamContactMapId.setTPriContact(getEntityManager().get(
					TPriContact.class, contact.get(j)));
			tkamContactMap.setId(tkamContactMapId);
			doSave(tkamContactMap);
		}
	}
	// 保存主营业务
	@SuppressWarnings("unchecked")
	public void saveBiztype(List<Long> biz, Long kamid) throws Exception {
		for (int j = 0; j < biz.size(); j++) {
			TKamBizMap tkamBizMap = new TKamBizMap();
			TKamBizMapId tkamBizMapId = new TKamBizMapId();
			tkamBizMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamBizMapId.setTKamBiztype(getEntityManager().get(
					TKamBiztype.class, biz.get(j)));
			tkamBizMap.setId(tkamBizMapId);
			doSave(tkamBizMap);
		}
	}
	
	//保存特殊航线备注///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public void saveSpecialAirline(List<TSpecialAirline> specialAirline,Long kamid){
		for (int i = 0; i < specialAirline.size(); i++) {
			TSpecialAirline sAirline = specialAirline.get(i);
			List<TSpaceDiscount> spaceDiscounts = sAirline.getTSpaceDiscounts();
			for (int j = 0; j < spaceDiscounts.size(); j++) {
				TSpaceDiscount sDiscount = spaceDiscounts.get(j);
				sDiscount.setTSpecialAirline(sAirline);
				doSave(sDiscount);
			}
			sAirline.setTKam(getEntityManager().get(TKam.class, kamid));
			doSave(sAirline);
		}
	}
	//保存增值服务 saveService
	@SuppressWarnings("unchecked")
	public void saveService(List<Long> serviceid, Long kamid) throws Exception {
		//过虑重复的id号
		for(int t=0;t<serviceid.size();t++){
			String sub1	= serviceid.get(t).toString();
			for(int r=t+1;r<serviceid.size();r++){
			  String sub2 = serviceid.get(r).toString();
				if(sub1.equals(sub2)){
					serviceid.remove(r);				
				}						
			}						
		}
		for (int j = 0; j < serviceid.size(); j++) {
			KamValueAddedServiceMap kamValueAddedServiceMap = new KamValueAddedServiceMap();
			KamValueAddedServiceMapId kamValueAddedServiceMapId = new KamValueAddedServiceMapId();
			kamValueAddedServiceMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			kamValueAddedServiceMapId.setValueAddedService(getEntityManager().get(
					ValueAddedService.class, serviceid.get(j)));
			kamValueAddedServiceMap.setId(kamValueAddedServiceMapId);
			doSave(kamValueAddedServiceMap);
		}

	}
	// 更新主营业务
	@SuppressWarnings("unchecked")
	public void updateBizType(List<Long> biz, Long kamid) throws Exception {
		//删除大客户就的主营业务
		String hql = "from TKamBizMap map where map.id.TKam.kamid=" + kamid;
		List<TKamBizMap> bizmap = getEntityManager().queryForListByHql(hql);
		if (bizmap.size() > 0) {
			getEntityManager().delete(bizmap);
		}
		//新增新的主营业务
		for (int j = 0; j < biz.size(); j++) {
			TKamBizMap tkamBizMap = new TKamBizMap();
			TKamBizMapId tkamBizMapId = new TKamBizMapId();
			tkamBizMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			tkamBizMapId.setTKamBiztype(getEntityManager().get(
					TKamBiztype.class, biz.get(j)));
			tkamBizMap.setId(tkamBizMapId);
			doSave(tkamBizMap);
		}
	}
	//更新 特殊航线备注
	@SuppressWarnings("unchecked")
	public void updateSpecialAirline(List<TSpecialAirline> specialAirline, Long kamid){
		if(specialAirline == null){
			return;
		}
		//删除旧的特殊航线
		String hql = "from TSpecialAirline ts where ts.TKam.kamid=" + kamid;
		List<TSpecialAirline> oldSpecialAirline = getEntityManager().queryForListByHql(hql);
		if (oldSpecialAirline.size() > 0) {
			getEntityManager().delete(oldSpecialAirline);
		}
		//增加新的特殊航线
		for (int i = 0; i < specialAirline.size(); i++) {
			TSpecialAirline sAirline = specialAirline.get(i);
			List<TSpaceDiscount> spaceDiscounts = sAirline.getTSpaceDiscounts();
			for (int j = 0; j < spaceDiscounts.size(); j++) {
				TSpaceDiscount sDiscount = spaceDiscounts.get(j);
				sDiscount.setTSpecialAirline(sAirline);
				doSave(sDiscount);
			}
			sAirline.setTKam(getEntityManager().get(TKam.class, kamid));
			doSave(sAirline);
		}
	}
	
	//更新增值服务
	@SuppressWarnings("unchecked")
	public void updateService(List<Long> serviceid, Long kamid) throws Exception {
		//过虑重复的id号
		for(int t=0;t<serviceid.size();t++){
			String sub1	= serviceid.get(t).toString();
			for(int r=t+1;r<serviceid.size();r++){
			  String sub2 = serviceid.get(r).toString();
				if(sub1.equals(sub2)){
					serviceid.remove(r);				
				}						
			}						
		}
		//把大客户以前对应的增值服务删除掉
		String hql = "from KamValueAddedServiceMap map where map.id.TKam.kamid=" + kamid;
		List<KamValueAddedServiceMap> servicemap = getEntityManager().queryForListByHql(hql);
		if (servicemap.size() > 0) {
			getEntityManager().delete(servicemap);
		}
		//新增增值服务
		for (int j = 0; j < serviceid.size(); j++) {
			KamValueAddedServiceMap kamValueAddedServiceMap = new KamValueAddedServiceMap();
			KamValueAddedServiceMapId kamValueAddedServiceMapId = new KamValueAddedServiceMapId();
			kamValueAddedServiceMapId.setTKam(getEntityManager().get(TKam.class, kamid));
			kamValueAddedServiceMapId.setValueAddedService(getEntityManager().get(
					ValueAddedService.class, serviceid.get(j)));
			kamValueAddedServiceMap.setId(kamValueAddedServiceMapId);
			doSave(kamValueAddedServiceMap);
		}
	}	
	// 放弃此大客户
	public String doCancle(Long kamid,Long operatid) {
		TKam kam = getTKamById(kamid);
		if (kam != null) {
			kam.setStatus(new Long(11));
			getEntityManager().save(kam);
			
			// 纪录操作日志
			Date currentTime = getEntityManager().getDbTime();
			TKamLog log = new TKamLog();
			log.setTKam(kam);
			log.setOperatorid(operatid);
			log.setOperatedate(currentTime);
			log.setOperatetype(new Long(12));
			getEntityManager().save(log);
			
			return "ok";
		} else {
			return null;
		}
	}
	
	/**
	 * 当前人有几张KAM带审批
	 * @param groupandrole
	 * @param accountid
	 * @author wangym 2010-09-16 修改
	 * @desprition 只查询当前用户做在组和权限的大客户列表
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> getKamToApprove(List<String> groupandrole,Long accountid) {
		List<Long> list=contractservice.getKamToApprove(groupandrole, accountid);
		List<Long> newlist = new ArrayList<Long>();
		List<Long> newlist2 = new ArrayList<Long>();
		
		for (Long id : list) {
			newlist.add(id);
			if (id == null)
				newlist2.add(Long.parseLong("0"));
			else
				newlist2.add(id);
		}
//		for (Object[] accountid : list) {			
//			newlist2.add(Long.parseLong(((BigDecimal) accountid[1]).toString()));
//		}
		if (newlist != null && newlist.size() > 0) {
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
			bqc.addExpressions(Restrictions.in("kamid", newlist));
			List<TKam> result = getEntityManager().query(bqc);
			for (int i = 0; i < result.size(); i++) {
				TKam kam = result.get(i);
				TFlowNodeCM node=get(TFlowNodeCM.class, kam.getActivenodeid());
				String group=node.getTPriGroup().getME_groupname();
				String role=node.getTPriRole().getME_rolename();
				kam.setActivenodestr(group+"的"+role);
				for(int j = 0; j < newlist.size(); j++){
					if(newlist.get(j) .toString().equals(kam.getKamid().toString())) 
						kam.setAccount(get(TPriAccountCM.class, newlist2.get(j)));
				} 
			}
			// 给TKam加载account以便显示"客户经理"一列
//			for (int i = 0; i < result.size(); i++) {
//				TKam kam = result.get(i);
//				sql = "select company.ACCOUNTID from t_kam kam,t_kam_company company,t_kam_cmp_map kcmap where"
//						+ " kam.kamid=kcmap.kamid and kcmap.companyid=company.companyid and kam.kamid="
//						+ kam.getKamid();
////				List managerid = getEntityManager().queryForListBySql(sql);
////				if (managerid.size() != 0 && managerid.get(0) != null) {
////					kam.setAccount(get(TPriAccountCM.class, new Long(managerid
////							.get(0).toString())));
////				} else {
////					TPriAccountCM a = new TPriAccountCM();
////					a.setME_namecn("暂无");
////				}
//			}
//			for (int i = 0; i < result.size(); i++) {
//				TKam kam = result.get(i);
//				sql = "from TFlowOperateLogCM log where log.flowtype=0 and log.flowtypeId ="
//						+ kam.getKamid()
//						+ " and log.ME_operatetype=0 order by log.ME_operatedate desc";
//				List<TFlowOperateLogCM> logs = getEntityManager()
//						.queryForListByHql(sql);
//				if (logs.size() != 0) {
//					Date submitDate = ((TFlowOperateLogCM) getEntityManager()
//							.queryForListByHql(sql).get(0)).getME_operatedate();
//					kam.setSubmitDate(DateUtils.getFormatDate(submitDate,
//							"yyyy-MM-dd"));
//				}
//			}
//			for (int i = 0; i < result.size(); i++) {
//				TKam kam = result.get(i);
//				sql = "from TFlowOperateLogCM log where log.flowtype=0 and log.flowtypeId ="
//						+ kam.getKamid()
//						+ " and log.ME_operatetype=1 order by log.ME_operatedate desc";
//				List<TFlowOperateLogCM> logs = getEntityManager()
//						.queryForListByHql(sql);
//				if (logs.size() != 0) {
//					Date submitDate = ((TFlowOperateLogCM) getEntityManager()
//							.queryForListByHql(sql).get(0)).getME_operatedate();
//					kam.setPassDate(DateUtils.getFormatDate(submitDate,
//							"yyyy-MM-dd"));
//				}
//			}
			return result;

		}
		return null;
	}

	// 校验唯一性
	@SuppressWarnings("unchecked")
	public String queryKamByKamno(String kamno) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		bqc.addExpressions(Restrictions.and(Restrictions.equal("delflag", 0l),
				Restrictions.like("kamno", kamno, MatchMode.START)));
		List<TKam> result = getEntityManager().query(bqc);
		if (result != null && result.size() > 0) {
			return "no";
		}
		return null;
	}

	// B2B数据库comCd格式化常量
	private static final String STR_FORMAT = "00000";

	@SuppressWarnings("unchecked")
	public List<TKamBiztype> getTKamBiztype() {
		String hql = "from TKamBiztype";
		return getEntityManager().queryForListByHql(hql);
	}

	// 同时在b2g数据库中新增大客户的信息
	@SuppressWarnings("unchecked")
	public void updateB2Bdata(TKam tkam, String random) {
		String sqlstring = "select t.kamno from TrCompVO t where t.kamno='"
				+ tkam.getKamno() + "'";
		List temp = getB2bEntityManager().queryForListByHql(sqlstring);
		if (temp.size() == 0) {
			// 在B2G上新增一个集团客户公司 B2GIN---
			StringBuffer hql = new StringBuffer();
			hql.append("select MAX(t.compCd) from TrCompVO as t where t.compCd < '91033'");
			String copmcd = (String) getB2bEntityManager().queryForObjectByHql(
					hql.toString());
			//added by ld 2010年8月30日 13:58:19 排除正式91033错误编号
			if(Integer.parseInt(copmcd) == 91032) {
				copmcd = (String) getB2bEntityManager().queryForObjectByHql("select MAX(t.compCd) from TrCompVO as t");
			}
			String compid = haoAddOne(copmcd);
			TrCompVO compVO = new TrCompVO();
			compVO.setCompCd(compid);
			compVO.setKamno(tkam.getKamno());
			compVO.setKamid(Integer.valueOf(tkam.getKamid().toString()));
			compVO.setCompNm(tkam.getKamnamecn());
			compVO.setCompEnm(tkam.getKamnameen());
			compVO.setAmrnk("3");
			compVO.setCashAm(1);
			compVO.setCredPd(0);
			// compVO.setCompAnm(tkam.getKamnamecn());
			compVO.setTravelCd("");
			Date currentTime = getEntityManager().getDbTime();
			compVO.setUpdDtm(currentTime);
			compVO.setShowempFg(true);
			compVO.setPrvtFg(true);
			compVO.setPaymodFg(true);
			compVO.setOverListpriceFg(true);
			compVO.setNoIbtinsFg(true);
			compVO.setHolidayFg(true);
			compVO.setDataSource("GM");
			compVO.setBudDtm(currentTime);
			//added by lingd 标识：2方或者3方客户
			compVO.setAgreeMentType(""+tkam.getAgreementtype().longValue());
			this.getB2bEntityManager().save(compVO);

			
			// yangpeng 2012-06-20
			// 获取前台注册是联系人资料，存入Trcompemp中
			String ysqlstr = "select customerid from t_kam_cntl_map where kamid = " +
					"(select kamid from t_kam t where t.kamno = '" + tkam.getKamno() + "') order by customerid asc";
			List customerIdList = getEntityManager().queryForListBySql(ysqlstr);
			TKamCustomer tKamCustomer = new TKamCustomer();
			if(null != customerIdList && customerIdList.size() > 0){
				BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamCustomer.class);
				bqc.addExpressions(Restrictions.equal("customerid", Long.valueOf(customerIdList.get(0).toString())));
				List<TKamCustomer> customerList = getEntityManager().query(bqc);
				tKamCustomer = (TKamCustomer)customerList.get(0);
			}else{
				//现在联系人两个录入，一种后台录入是录入到t_kam_contact,一种前台录入录入到t_kam_customer
				//这里对于后者取不到值的情况下，再到前者去取 
				try {
					List<TKamCustomer> result;
					result = queryCustomerByKamId(tkam.getKamid(), 1L);
					if(result != null && result.size() != 0)
						tKamCustomer = result.get(0);
				} catch (ServiceException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(); 
				}				
			}
			

			 
			
			// added by nizq Date:20090717 for: 新增一个集团客户管理超级帐号
			Trcompemp compemp = new Trcompemp();
			String compEmpCd = "";
			compEmpCd = compid + "0001";
			compemp.setCompCd(compid);
			compemp.setCompempCd(compEmpCd); // 管理员的员工号为集团客户号
			if(tKamCustomer.getCustomerid() != null)
			compemp.setCustomerid(tKamCustomer.getCustomerid().intValue());
			compemp.setBrthDt(null);
			compemp.setCipFg("1");
			//compemp.setCompempCnmf("管理员");
			compemp.setCompempCnml(tKamCustomer.getNamecn()); //姓名
			compemp.setCompempEnmf("admin");
			compemp.setCompempEnml("super");
			compemp.setCompempTi("MR");
			compemp.setDataSource("GM");
			compemp.setAccountProperty("1");
			compemp.setDeptNm("");
			compemp.setIdNo(null);
			compemp.setKamno(tkam.getKamno());
			compemp.setLastModified(new Date());
			compemp.setMobile(tKamCustomer.getMobile()); //手机
			compemp.setPassNo(null);
			compemp.setPassVldeDt(null);
			compemp.setPostNm("管理员");
			getB2bEntityManager().save(compemp); // 插入B2G TRCOMPEMP
			hibernate.flush();
			// 插入B2G 登陆帐号
//			String serachsql = "select max(src_id) from TRUSR  t where t.SRC_CLS='C'";
//			Integer userId = Integer.valueOf(getB2bEntityManager()
//					.queryForStringBySql(serachsql));
//			userId = userId + 1;
			String tempPassWord = random;
			String password = PassWord.encodeNow(tempPassWord);
			String sql = "INSERT INTO TRUSR(BUD_DTM, MOCRED_FG, USR_ID, USR_PASSWD, CRED_AUTHO_ID, ISSUE_FG, SRC_ID,SRC_CLS,USR_VAT,USR_CNM,USR_ENM,NICK_NAME) VALUES ("
					+ "convert(datetime,'"
					+ DateUtils.getFormatDate(new Date(), "yyyy-MM-dd")
					+ "'),"
					+ "'0'"
					+ ","
					+ "'"
					+ "C"
					+ compEmpCd
					+ "'"
					+ ","
					+ "'"
					+ password
					+ "'"
					+ ","
					+ "''"
					+ ","
					+ "'1'"
					+ ",'"
					+ compEmpCd
					+ "',"
					+ "'C'"
					+ ","
					+ "'0'"
					+ ",N'"
					+ compemp.getCompempCnml()
					+ compemp.getCompempCnmf()
					+ "',N'"
					+ compemp.getCompempEnml()
					+ compemp.getCompempEnmf() + "','" + tkam.getKamno() + "')";
			getB2bEntityManager().executeUpdateBySql(sql);
			try {
				contractservice.sendPassWordForKas(tkam.getKamid(), random);
			} catch (MessagingException e) {
			
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public String nameExisted(String kamname, Long kamid) {
		String hql = "from TKam kam where kam.delflag = 0 and kam.kamid<>"
				+ kamid
				+ " and kam.status<>11 and kam.status<>12 and kam.kamnamecn='"
				+ kamname + "'";
		List<TKam> kams = getEntityManager().queryForListByHql(hql);
		if (kams.size() != 0) {
			return "中文名重复，存在同中文名的大客户，客户编号:" + kams.get(0).getKamno();
		}
		hql = "from TKam kam where kam.delflag = 0 and kam.kamid<>" + kamid
				+ " and( kam.status=11 or kam.status=12 ) and kam.kamnamecn='"
				+ kamname + "'";
		kams = getEntityManager().queryForListByHql(hql);
		if (kams.size() != 0) {
			return "中文名重复，存在同中文名的大客户在历史库中，客户编号:" + kams.get(0).getKamno();
		}
		return "success";
	}

	public boolean tempStatus(Long kamid) {
		String hql = "from TKamContract c where c.instancefileurl =2 and c.delflag=0 and c.isagreement<>1 and c.TKam.kamid="
				+ kamid;
		if (getEntityManager().queryForListByHql(hql).size() != 0)
			return true;
		else
			return false;
	}

	public boolean hasContract(Long kamid) {
		String hql = "select count(*) from TKamContract c where c.TKam.kamid="
				+ kamid + " and c.delflag=0 and c.isagreement<>1";
		Integer count = getEntityManager().queryForIntegerByHql(hql);
		if (count.intValue() > 0) {
			return true;
		}
		return false;
	}

	// 根据basispolicyid查询前期奖励政策
	@SuppressWarnings("unchecked")
	public List<TBasispolicyPerbonusMap> queryPerbonusByBasispolicyid(
			String basispolicyid, Long perbonustype) {
		BeanQueryCondition bqc1 = BeanQueryCondition.forClass(TBasispolicyPerbonusMap.class);
		bqc1.addExpressions(Restrictions.equal(
				"TIncentiveBasispolicy.basispolicyid", new Long(basispolicyid)));
		bqc1.addExpressions(Restrictions.equal(
				"TIncentivePerbonus.perbonustype", perbonustype));
		bqc1.addInitFields("TIncentivePerbonus");
		// bqc1.addInitFields("TIncentiveBasispolicy");
		List<TBasispolicyPerbonusMap> perbonuslist = getEntityManager().query(bqc1);
		return perbonuslist;
	}

	// 根据basispolicyid查询后返政策
	@SuppressWarnings("unchecked")
	public List<TPolicyPostbonusMap> queryPostbonusByBasispolicyid(
			String basispolicyid) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TPolicyPostbonusMap.class);
		bqc.addExpressions(Restrictions.equal(
			"TIncentiveBasispolicy.basispolicyid", new Long(basispolicyid)));
		bqc.addInitFields("TIncentivePostbonus");
		// bqc1.addInitFields("TIncentiveBasispolicy");
		List<TPolicyPostbonusMap> postbonuslist = getEntityManager().query(bqc);
		return postbonuslist;
	}
	
	/**
	 * 查询大客户前返折扣列表的sql
	 * @param tkamDto
	 * @param p
	 * @param groupId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings("unchecked")
	private StringBuffer queryKamPerbounsListSql(TKamDto tkamDto,OrderablePagination p, Long groupId, String type) throws ServiceException {
		StringBuffer sql = null;
		if("query".equals(type)) {
			sql = getQuerySql();
		}else {
			sql = getDownSql();
		}
	   sql.append(" where t1.kamid in (select distinct map.kamid from t_kam_contract_awards_map map, t_use_awards_perbonus p where p.bonusid = map.bonusid and map.policytype=1 ) ");
		   
//		List<Long> groupidList = contractservice.getAllgroupByGroup(groupId);
//		if (null != groupidList && groupidList.size() != 0) {
//			sql.append(" and t1.groupid in (" + groupidList.get(0));
//			for(int i = 1; i < groupidList.size(); i++) {
//				sql.append(" ," + groupidList.get(i));
//			}
//			sql.append(")");
//		} else {
//			return null;
//		}
		//按groupid转化为deptid去查询
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			for(int i = 0; i < deptList.size(); i++) {
				if(i == 0)
					sql.append(" and t1.departmentid in (" +deptList.get(i).getME_departmentid());
				else 
					sql.append(" , " + deptList.get(i).getME_departmentid());
			}
			sql.append(")");
		} else {
			return null;
		}
		
		if (StringUtils.hasText(tkamDto.getKamNo())) {
			sql.append(" and t1.kamno like '" + tkamDto.getKamNo()+ "%'");
		}
		if(StringUtils.hasText(tkamDto.getKamName())) {
			sql.append(" and t1.kamnamecn like '" + tkamDto.getKamName() + "'");
		}
		if(tkamDto.getStatus() == null || tkamDto.getStatus().longValue() == 30) {
			//默认是已审核+已审批
			sql.append(" and (t1.status = 7 or t1.status = 15)");
		}else {
			sql.append(" and t1.status = " + tkamDto.getStatus() );
		}
		//合同有效期
		if(!StringUtils.isBlank(tkamDto.getValiDate())) {
			sql.append(" and t2.validatedate >=  to_date('" + tkamDto.getValiDate().trim() + "','yyyy-mm-dd')");
		}
		if(!StringUtils.isBlank(tkamDto.getInValiDate())) {
			sql.append(" and t2.invalidatedate <=  to_date('" + tkamDto.getInValiDate().trim() + "','yyyy-mm-dd')+1");
		}
		//审核日期
		if(!StringUtils.isBlank(tkamDto.getQDate1())) {
			sql.append(" and logdate.operatedate >=  to_date('" + tkamDto.getQDate1().trim() + "','yyyy-mm-dd')");
		}
		if(!StringUtils.isBlank(tkamDto.getQDate2())) {
			sql.append(" and logdate.operatedate <=  to_date('" + tkamDto.getQDate2().trim() + "','yyyy-mm-dd')+1");
		}
		
		Sorter[] sorters = null;
		if(p != null)
			sorters = p.getSorters();
		if (ObjectUtils.isEmpty(sorters)) {
			sql.append(" order by t1.kamid desc");
		} else {
			sql.append(" order by ");
			for(int i = 0; i < sorters.length; i++) {
				 sql.append(" " + sorters[i].getAttributeName() + " " + sorters[i].getSortType());
			}
		}
		return sql;
	}

	//查询前返列表的sql
	private StringBuffer getQuerySql() {	
		//获取审批日期 swDate
		StringWriter swDate = null;
		try {
			swDate = new StringWriter();
			VelocityHelper.output(null, swDate, "kamperbonus_approvedDate_sql.vm");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(swDate != null) {
				try {
					swDate.close();
				} catch (Exception e) {}
			}
		}  
		StringBuffer sql = new StringBuffer();
		sql.append("select t1.kamno,t1.kamnamecn,t2.validatedate,t2.invalidatedate,t1.remark,t1.kamid,dept.deptname,logdate.operatedate, account.namecn ")
		   .append("from ")
		   .append("(select kam.kamid,kam.kamno,kam.kamnamecn,kam.groupid,kam.status,kam.remark,kam.departmentid, kam.contractid from t_kam kam where kam.kamid <> -1 and kam.delflag = 0) t1 left join ")
		   .append("(select con.kamid,con.validatedate,con.invalidatedate, con.contractid from t_kam_contract con where con.kamid<> -1 ) t2 on t1.contractid=t2.contractid ")
		   .append(" left join t_pri_dept dept on dept.departmentid = t1.departmentid")
		   .append(" left join ( " + swDate.toString() + ") logdate on t1.kamid = logdate.flowtypeid ")
		   .append(" left join ( select tkcmap.kamid kamid, min(tkcmap.companyid) companyid from t_kam_cmp_map tkcmap group by tkcmap.kamid) compId on compId.kamid = t1.kamid  ")
		   .append(" left join t_kam_company comp on comp.companyid = compId.companyid ")
		   .append(" left join t_pri_account account on account.accountid = comp.accountid ");
		return sql;
	}
	
	//下载excel的sql
	private StringBuffer getDownSql() {
	   //获取sql--多行奖励政策 sw
	   //获取审批日期 swDate
	   StringWriter sw = null;
	   StringWriter swDate = null;
	   try {
			sw = new StringWriter();
			swDate = new StringWriter();
			VelocityHelper.output(null, sw, "kamperbonus_sql.vm");
			VelocityHelper.output(null, swDate, "kamperbonus_approvedDate_sql.vm");
	   } catch (Exception e) {
			e.printStackTrace();
	   } finally {
			if(sw != null) {
				try {
					sw.close();
				} catch (Exception e) {}
			}
			if(swDate != null) {
				try {
					swDate.close();
				} catch (Exception e) {}
			}
	   }  
	   StringBuffer sql = new StringBuffer();
	   sql.append("select t1.kamno,t1.kamnamecn,t2.validatedate,t2.invalidatedate,t1.remark,t1.kamid,dept.deptname,logdate.operatedate, account.namecn, bonus.cabin, bonus.remark1 ")
		   .append(" from ")
		   .append("(select kam.kamid,kam.kamno,kam.kamnamecn,kam.groupid,kam.status,kam.remark,kam.departmentid, kam.contractid from t_kam kam where kam.kamid <> -1 and kam.delflag = 0) t1 left join ")
		   .append("(select con.kamid,con.validatedate,con.invalidatedate, con.contractid from t_kam_contract con where con.kamid<> -1 ) t2 on t1.contractid=t2.contractid ")
		   .append(" left join t_pri_dept dept on dept.departmentid = t1.departmentid")
		   .append(" left join (" + swDate.toString() + ") logdate on t1.kamid = logdate.flowtypeid ")
		   .append(" left join ( select tkcmap.kamid kamid, min(tkcmap.companyid) companyid from t_kam_cmp_map tkcmap group by tkcmap.kamid) compId on compId.kamid = t1.kamid  ")
		   .append(" left join t_kam_company comp on comp.companyid = compId.companyid ")
		   .append(" left join (" + sw.toString() + ") bonus on bonus.kamid = t1.kamid ")
		   .append(" left join t_pri_account account on account.accountid = comp.accountid ");
	   return sql;
	}
	
	/**
	 * 大客户前返折扣列表
	 * @param tkamDto
	 * @param p
	 * @param groupId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	public List<TKam> queryKamPerbounsList(TKamDto tkamDto, OrderablePagination p,Long groupId) throws ServiceException {
		List<TKam> kams = new ArrayList<TKam>();
		//查询的sql
		StringBuffer hql = queryKamPerbounsListSql(tkamDto, p, groupId, "query");
		List<Object[]> objs = getEntityManager().queryForListByHibernateSql(hql.toString(), p, null);
		TKam kam;
		Object[] obj;
		String hqlAwards = null;
		List<TUseAwardsPerBonus> list = null;
		String remark = null;
		StringBuffer cangWei = null;
		TPriAccountCM account = null;
		for(int i = 0; i < objs.size(); i++) {
			kam = new TKam();
			obj = objs.get(i);
			kam.setKamno((String)obj[0]);
			kam.setKamnamecn((String)obj[1]);
			kam.setValiDate((Date)obj[2]);
			kam.setInValiDate((Date)obj[3]);
			kam.setRemark((String)obj[4]);
			kam.setKamid(((BigDecimal)obj[5]).longValue());
			hqlAwards = "from TUseAwardsPerBonus per where per.bonusId in (select map.bonusid from TUseContractAwardsMap map where map.kamid='" + obj[5] + "')";
			list = getEntityManager().queryForListByHql(hqlAwards);
			kam.setAwardsSet(list);
			
			remark = "";
			cangWei = new StringBuffer();
			for(int j = 0; j < list.size(); j++) {
				// 舱位
				for(TUseAwardsPerBonusCanbin vo : list.get(j).getClassDiscVos()) {
					//国内
					if(vo.getAreaType() != null && vo.getAreaType() == 0) {
						cangWei.append(vo.getCanbin() + vo.getRealDiscount() + " / ");
					}
				}
				//备注
				if(list.get(j).getRouteDes() != null) {
					remark = remark + list.get(j).getRouteDes() + "，";
				}
			}
			if(cangWei.toString().indexOf("/") != -1)
				kam.setAwardsCW(cangWei.toString().substring(0, cangWei.length() - 2));
			if(remark.indexOf("，") != -1)
				kam.setRemarkAward(remark.substring(0, remark.length() - 1));
			kam.setDeptName((String)obj[6]);
			kam.setLogOperatedate((Date)obj[7]);
			kam.setIataOfficecode(this.getTkamIataCodeOfficeCode(obj[5].toString()));
			//客户经理
			account = new TPriAccountCM();
			if(obj[8] != null)
				account.setME_namecn((String)obj[8]);
			else
				account.setME_namecn("暂无");
			kam.setAccount(account);
			kams.add(kam);
		}
		return kams;
	}
	
	/**
	 * 打印户前返折扣列表
	 * @param tkamDto
	 * @param p
	 * @param groupId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> queryDownKamPerbouns(TKamDto tkamDto, OrderablePagination p,Long groupId) throws ServiceException {
		List<TKam> kams = new ArrayList<TKam>();
		List<Object[]> objs = getEntityManager().queryForListByHibernateSql(queryKamPerbounsListSql(tkamDto, p, groupId, "down").toString(), p, null);
		TKam kam;
		Object[] obj;
		TPriAccountCM account = null;
		String[] remarks = null;
		Object[] o = null;
		Set<String> s = null;
		String remark = null;
		Map map = null;
		for(int i = 0; i < objs.size(); i++) {

			kam = new TKam();
			obj = objs.get(i);
			kam.setKamno((String)obj[0]);
			kam.setKamnamecn((String)obj[1]);
			kam.setValiDate((Date)obj[2]);
			kam.setInValiDate((Date)obj[3]);
			kam.setRemark((String)obj[4]);
			kam.setKamid(((BigDecimal)obj[5]).longValue());
			kam.setDeptName((String)obj[6]);
			kam.setLogOperatedate((Date)obj[7]);
			//客户经理
			account = new TPriAccountCM();
			if(obj[8] != null)
				account.setME_namecn((String)obj[8]);
			else
				account.setME_namecn("暂无");
			kam.setAccount(account);
			kam.setAwardsCW((String) obj[9]);
			//政策备注
			if((String)obj[10] != null) {
				remarks = ((String)obj[10]).split("#");
				map = new HashMap();
				for(String str : remarks) {
					if(str == "")
						continue;
					map.put(str.trim(), str.trim());
				}
				if(map.size() > 0 ) {
					o = map.keySet().toArray();
					for(int j = 0; j < o.length; j++) {
						if(j == 0) {
							remark = "";
							remark = remark + o[j];
						}else {
							remark = remark + ";" + o[j];
						}
					}
					kam.setRemarkAward(remark);
				}
			}

			
			kams.add(kam);
		}
		return kams;
	}
	
	/**
	 * 查询tkam对应的奖励政策和office
	 * @param list
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	public List<TKam> queryTkamAwardsOffice(List<TKam> list) {
		//-------------存储过程调用-----------------//
		int size = list.size();
		Object[] kamids = new Object[size];
		for(int i = 0;i < size; i++){
			kamids[i] = list.get(i).getKamid();
		}
		//System.out.println("连接获取开始时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
		//获取连接
		Connection conn = null;//hibernate.getSessionFactory().getCurrentSession().connection();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		//从连接池中获取连接
    	//conn = hibernate.getSessionFactory().getCurrentSession().connection();
	    try {
	    	if(SystemConfiguration.getPropertyValue("com.cares.perbonus.prec") == null ||
	    			"test".equals(SystemConfiguration.getPropertyValue("com.cares.perbonus.prec")))
	    		conn = DriverManager.getConnection("jdbc:oracle:thin:@172.20.18.152:1521:b2btest","gm5","gm5");
	    	else {
	    		conn = DriverManager.getConnection("jdbc:oracle:thin:@172.20.4.18:1521:kams","gm","gm123");
	    	}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		//System.out.println("连接获取结束时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
		OracleCallableStatement cstmt = null;
		try {
			cstmt = (OracleCallableStatement )conn.prepareCall("{call prc_kam_perbouns_list1118(?,?)}");
			//输入参数 
			ArrayDescriptor descriptorIn = ArrayDescriptor.createDescriptor("type_array_kamid".toUpperCase(), conn);
			ARRAY vArray = new ARRAY(descriptorIn, conn, kamids);
			cstmt.setArray(1, vArray);
			cstmt.registerOutParameter(2, OracleTypes.ARRAY,"type_array_result".toUpperCase());
			//System.out.println("数据获取开始时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
			cstmt.execute();
			//System.out.println("数据获取结束时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
			ARRAY array = cstmt.getARRAY(2);
			//System.out.println("数据处理开始时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
			if(array != null){
				TKam kam = null;
				Datum[] datas = array.getOracleArray();
				for (int i = 0; i < datas.length; i++){
					Datum[] personAttributes = ((STRUCT) datas[i]).getOracleAttributes();
					for(int j = 0; j < size; j++){
						kam = list.get(j);
						if(Long.valueOf(new String(personAttributes[0].getBytes()).trim()) .equals(list.get(j).getKamid()) ){
//							if(personAttributes[1] != null){
//								String cangwei = new String(personAttributes[1].getBytes());
//								if(cangwei.length()>1){
//									kam.setAwardsCW(cangwei.substring(0, cangwei.length()-1));
//								}
//							}
							if(personAttributes[2] != null ){
								String code = new String(personAttributes[2].getBytes());
								if(code.length() > 1){
									kam.setIataOfficecode(code.substring(0, code.length()-1));
								}
							}
//							if(personAttributes[3] != null){
//								String remark = new String(personAttributes[3].getBytes());
//								if(remark.length() > 1){
//									kam.setRemark(remark.substring(0, remark.length()-1));
//								}
//							}
						}
					}
				}
			}
			//System.out.println("数据处理结束时间为："+DateUtils.getFormatDate(getCurrentTime(),"yyyy-MM-dd HH:mm:ss"));
		} catch (SQLException e) {
			try {
				conn.close();
				cstmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			try {
				conn.close();
				cstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	/**
	 * 获取某大客户的iatacode和officecode
	 * @param kamid
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private String getTkamIataCodeOfficeCode(String kamid) {
		
		String sql = "select t.departmentid from t_kam_agent_map t where t.kamid=' "
			+ kamid + "'";
		List<Object> tpriDeptCM = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> list = new ArrayList<Long>();
		for (Object id : tpriDeptCM) {
			list.add(Long.parseLong(((BigDecimal) id).toString()));
		}
		StringBuffer sb = new StringBuffer();
		String code = null;
		for(Long deptid: list) {
			sql = "select t.iatacode from t_info_iata t where t.departmentid="
				+ deptid;
			List<String> iatacode = getEntityManager().queryForListBySql(sql);
			if (null != iatacode && iatacode.size() > 0) {
				code = iatacode.get(0).toString();
				if(iatacode.size() > 1)
					for(String str : iatacode)
						code = code +  "，" +  str;
			}
			if(code == null) {
				code = "---";
			}
			sb.append(code+"/");
			
			code = null;
			sql = "select t.officecode from t_info_office t where t.departmentid="
				+ deptid;
			List<String> officecode = getEntityManager().queryForListBySql(sql);
			if (null != officecode && officecode.size() > 0) {
				code = officecode.get(0).toString();
				if(officecode.size() > 1)
					for(String str : officecode)
						code = code +  "，" + str;
			}
			if(code == null) {
				code = "---";
			}
			sb.append(code+"；");
		}
		if(sb.length() > 0) {
			return sb.toString().substring(0,sb.length()-1);
		}
		return sb.toString();
	}
	
	
	/**
	 * 根据kaiid取得前返折扣列表
	 */
	@SuppressWarnings("unchecked")
	public String queryKamPerbonusByKamid(Long kamid) throws ServiceException {
		StringBuffer sql = new StringBuffer();
		sql.append("select dis.canbin,dis.realdiscount,per.routedes ")
		   .append("from ")
		   .append("t_use_awards_perbonus per left join t_use_awards_perbonus_canbin dis on per.bonusid=dis.bonusid ")
		   .append("where per.bonusid in (select map.bonusid from t_kam_contract_awards_map map where map.kamid= ")
		   .append(kamid);
		@SuppressWarnings("unused")
		List<Object[]> objs = getEntityManager().queryForListBySql(sql.toString());
		
		return "";
	}
	/**
	 * 查询最近已审批信息表
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> queryTKamApproved(TKamDto tkamDto, OrderablePagination p,
			Long groupId) throws ServiceException {
		//查询的sql
		StringBuffer hql = queryTKamApprovedSql(tkamDto, p, groupId);
		if(hql == null)
			return new ArrayList<TKam>();
		List<Object[]> objs = getEntityManager().queryForListByHibernateSql(hql.toString(), p, null);
		
		List<TKam> kams = new ArrayList<TKam>();
		TKam kam ;
		Object[] obj;
		for(int i = 0; i < objs.size(); i++) {
			kam = new TKam();
			obj = objs.get(i);
			kam.setKamid(((BigDecimal)obj[0]).longValue());
//			//客户经理
//			Long kamid = kam.getKamid();
//			String sql = "select map.COMPANYID from t_kam_cmp_map map where map.KAMID="
//					+ kamid + " order by map.COMPANYID asc";
//			List companyIds = getEntityManager().queryForListBySql(sql);
//			Long companyId = null;
//			if (companyIds.size() != 0)
//				companyId = new Long(companyIds.get(0).toString());
//			Long accountid = null;
//			if (companyId != null)
//				accountid = get(TKamCompanyCM.class, companyId).getAccountId();
//			if (null != accountid) {
//				TPriAccountCM account = get(TPriAccountCM.class, accountid);
//				kam.setAccount(account);
//			} else {
//				TPriAccountCM account = new TPriAccountCM();
//				account.setME_namecn("暂无");
//				kam.setAccount(account);
//			}
			
			kam.setKamno((String)obj[1]);
			kam.setKamnamecn((String)obj[2]);
			kam.setKamnameen((String)obj[3]);
//			if(obj[4] != null)
//				kam.setCompanytype(((BigDecimal)obj[4]).longValue());
			if(obj[4] != null)
				kam.setStatus(((BigDecimal)obj[4]).longValue());
//			kam.setOperatorName((String)obj[6]);
			kam.setDeptName((String)obj[5]);
			kam.setLogOperatedate((Date)obj[6]);
//			kam.setCompanytypestr((String)obj[7]);
			kam.setValiDate((Date)obj[7]);
			kam.setInValiDate((Date)obj[8]);
			kams.add(kam);
		}
		return kams;
	}

	/**
	 * 查询近期已审批信息表的sql
	 * @param tkamDto
	 * @param p
	 * @param groupId
	 * @return
	 * @throws ServiceException
	 */
	@SuppressWarnings("unchecked")
	private StringBuffer queryTKamApprovedSql(TKamDto tkamDto,OrderablePagination p, Long groupId) throws ServiceException {
			
		StringBuffer hql1 = new StringBuffer("select kam.kamid, kam.kamno, kam.kamnamecn, kam.kamnameen, kam.status, dept.deptname, "
				+ " temp.operatedate, t2.validatedate, t2.invalidatedate from t_kam kam " 
				+ " left join t_pri_dept dept on kam.departmentid = dept.departmentid "
				+ " left join T_Pri_Account account on kam.operateid = account.accountid "
				+ " left join ( select log.flowtypeId as kamid, max(log.operatedate) as operatedate"
				+ "	from t_flow_operate_log log group by log.flowtypeId) temp"
		        + " on kam.kamid = temp.kamid " 
		        + " left join (select con.kamid,con.validatedate,con.invalidatedate, con.contractid from t_kam_contract con where con.kamid<> -1 ) t2 on kam.contractid=t2.contractid "
		        + " where kam.status = 7 ");
		
		StringBuffer hql2 = new StringBuffer("select kam.kamid, kam.kamno, kam.kamnamecn, kam.kamnameen, kam.status, dept.deptname, "
				+ " temp.operatedate, t2.validatedate, t2.invalidatedate from t_kam kam " 
				+ " left join t_pri_dept dept on kam.departmentid = dept.departmentid "
				+ " left join T_Pri_Account account on kam.operateid = account.accountid "
				+ " left join ( select logg.kamid , logg.operatedate from (select log.flowtypeId as kamid, log.operatedate, "
				+ "	row_number() over(partition by flowtypeid order by operatedate desc) as rn "
          		+ "	from t_flow_operate_log log ) logg where logg.rn = 2 ) temp "
		        + " on kam.kamid = temp.kamid " 
		        + " left join (select con.kamid,con.validatedate,con.invalidatedate, con.contractid from t_kam_contract con where con.kamid<> -1 ) t2 on kam.contractid=t2.contractid "
		        + " where  kam.status = 15  ");
//			List<Long> groupidList = contractservice.getAllgroupByGroup(groupId);
//			if (null != groupidList && groupidList.size() != 0) {
//				hql.append(" and kam.groupid in (" + groupidList.get(0));
//				for(int i = 1; i < groupidList.size(); i++) {
//					hql.append(" ," + groupidList.get(i));
//				}
//				hql.append(")");
//			} else {
//				return null;
//			}
		//按groupid转化为deptid去查询
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			for(int i = 0; i < deptList.size(); i++) {
				if(i == 0) {
					hql1.append(" and kam.departmentid in (" +deptList.get(i).getME_departmentid());
					hql2.append(" and kam.departmentid in (" +deptList.get(i).getME_departmentid());
				}
				else {
					hql1.append(" , " + deptList.get(i).getME_departmentid());
				    hql2.append(" , " + deptList.get(i).getME_departmentid());
				}
			}
			hql1.append(")");
			hql2.append(")");
		} else {
			return null;
		}
		
		// 按营业部查询
		String querydeptname = tkamDto.getDeptname();
		if (querydeptname != null && !querydeptname.equals("")) {
			String sql = "select t.departmentid from t_pri_dept t where t.depttype='0' and t.deptname like '%"
					+ querydeptname + "%'";
			List<Object> listlist = getEntityManager().queryForListBySql(sql);
			List<Long> newlist1 = new ArrayList<Long>();
			for (Object id : listlist) {
				newlist1.add(Long.valueOf((((BigDecimal) id).toString())));
			}
			if (newlist1 != null && newlist1.size() > 0) {
					hql1.append(" and dept.departmentid in (" + newlist1.get(0));
					hql2.append(" and dept.departmentid in (" + newlist1.get(0));
				for(int i = 1; i < newlist1.size(); i++) {
					hql1.append(" ," + newlist1.get(i));
					hql2.append(" ," + newlist1.get(i));
				}
				hql1.append(")");
				hql2.append(")");
			}
		}
		
		// 按代理人查询
		String queryagent = tkamDto.getAgent();
		if (StringUtils.hasText(queryagent)) {
			List<Long> list = relationAssessService.getKamByAgent(queryagent);
			if (null != list && list.size() != 0) {
				hql1.append(" and kam.kamid in (" + list.get(0));
				hql2.append(" and kam.kamid in (" + list.get(0));
				for(int i = 1; i < list.size(); i++) {
					hql1.append(" ," + list.get(i));
					hql2.append(" ," + list.get(i));
				}
				hql1.append(")");
				hql2.append(")");
			} else
				return null;
		}
		// 按联系人查询
		String querycontact = tkamDto.getContact();
		if (StringUtils.hasText(querycontact)) {
			List<Long> list = relationAssessService
					.getKamByCustomer(querycontact);
			if (null != list && list.size() != 0) {
				hql1.append(" and kam.kamid in (" + list.get(0));
				hql2.append(" and kam.kamid in (" + list.get(0));
				for(int i = 1; i < list.size(); i++) {
					hql1.append(" ," + list.get(i));
					hql2.append(" ," + list.get(i));
				}
				hql1.append(")");
				hql2.append(")");
			} else
				return null;
		}
		// 按客户经理查询
		String queryaccount = tkamDto.getAccount();
		if (StringUtils.hasText(queryaccount)) {
			List<Long> list = getKamByManagerId(queryaccount);
			if (null != list && list.size() != 0) {
				hql1.append(" and kam.kamid in (" + list.get(0));
				hql2.append(" and kam.kamid in (" + list.get(0));
				for(int i = 1; i < list.size(); i++) {
					hql1.append(" ," + list.get(i));
					hql2.append(" ," + list.get(i));
				}
				hql1.append(")");
				hql2.append(")");
			} else
				return null;
		}
		
		if (StringUtils.hasText(tkamDto.getKamNo())) {
			hql1.append(" and kam.kamno like '" + tkamDto.getKamNo()+ "%'");
			hql2.append(" and kam.kamno like '" + tkamDto.getKamNo()+ "%'");
		}
		if(tkamDto.getCompanyType() != null) {
			hql1.append(" and kam.companytype = " + tkamDto.getCompanyType());
			hql2.append(" and kam.companytype = " + tkamDto.getCompanyType());
		}
		if(tkamDto.getAgreementtype() != null) {
			hql1.append(" and kam.agreementtype = " + tkamDto.getAgreementtype());
			hql2.append(" and kam.agreementtype = " + tkamDto.getAgreementtype());
		}
		hql1.append(" and kam.delflag = 0");
		hql2.append(" and kam.delflag = 0");
		if(StringUtils.hasText(tkamDto.getKamName())) {
			hql1.append(" and (kam.kamnamecn like '" + tkamDto.getKamName() + "' or kam.kamnameen like '" + tkamDto.getKamName() + "')");
			hql2.append(" and (kam.kamnamecn like '" + tkamDto.getKamName() + "' or kam.kamnameen like '" + tkamDto.getKamName() + "')");
		}
		if(tkamDto.getStatus() == null || tkamDto.getStatus().longValue() == 30) {
			//默认是已审核+已审批
			hql1.append(" and (kam.status = 7 or kam.status = 15)");
			hql2.append(" and (kam.status = 7 or kam.status = 15)");
		}else {
			hql1.append(" and kam.status = " + tkamDto.getStatus() );
			hql2.append(" and kam.status = " + tkamDto.getStatus() );
		}
		//审核时间
//			String operateDateStr = this.queryTkamOperateDateSql(tkamDto);
		if(!StringUtils.isBlank(tkamDto.getQDate1())) {
			hql1.append(" and temp.operatedate >=  to_date('" + tkamDto.getQDate1().trim() + "','yyyy-mm-dd')");
			hql2.append(" and temp.operatedate >=  to_date('" + tkamDto.getQDate1().trim() + "','yyyy-mm-dd')");
		}
		if(!StringUtils.isBlank(tkamDto.getQDate2())) {
			hql1.append(" and temp.operatedate <=  to_date('" + tkamDto.getQDate2().trim() + "','yyyy-mm-dd')+1");
			hql2.append(" and temp.operatedate <=  to_date('" + tkamDto.getQDate2().trim() + "','yyyy-mm-dd')+1");
		}
		
		StringBuffer sql = new StringBuffer(" select tempall.kamid, tempall.kamno, tempall.kamnamecn, tempall.kamnameen, tempall.status, tempall.deptname," 
				+ " tempall.operatedate, tempall.validatedate, tempall.invalidatedate from ("   + hql1.toString() + " union " + hql2.toString() + " ) tempall " );
		
		Sorter[] sorters = p.getSorters();
		if (ObjectUtils.isEmpty(sorters)) {
			sql.append(" order by tempall.kamid desc");
		} else {
			sql.append( " order by ");
			for(int i = 0; i < sorters.length; i++) {
				sql.append(" " + sorters[i].getAttributeName() + " " + sorters[i].getSortType());
			}
		}
		return sql;

	}

	/**
	 * 根据审核日期查询记录
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public String queryTkamOperateDateSql(TKamDto tkamDto) {
//		90客户则查询kam表中的operatedate字段
//		StringBuffer hqlKam = new StringBuffer("select t.kamid from TKam t where t.kamno like '90%' ");
		//非90客户则查询log表中的operatedate字段
		StringBuffer hqlLog = new StringBuffer("select distinct(log.flowtypeId) from t_flow_operate_log log where 1=1" );
		if(!StringUtils.isBlank(tkamDto.getQDate1())) {
			hqlLog.append(" and log.operatedate >=  to_date('" + tkamDto.getQDate1().trim() + "','yyyy-mm-dd')");
		}
		if(!StringUtils.isBlank(tkamDto.getQDate2())) {
			hqlLog.append(" and log.operatedate <=  to_date('" + tkamDto.getQDate2().trim() + "','yyyy-mm-dd')+1");
		}
		if(StringUtils.isBlank(tkamDto.getQDate1()) && StringUtils.isBlank(tkamDto.getQDate2())) 
			return null;
		else {
			return hqlLog.toString();
		}
	}
	
	public void superUpdatePolicyGroup(TKam tkam, Long userid, Long groupid,
			Long roleid, String remark) throws Exception {
		TKamLog log = new TKamLog();
		log.setTKam(tkam);
		log.setOperatorid(userid);
		log.setOperatedate(getCurrentTime());
		log.setOperatetype(4l);
		getEntityManager().save(log);
		contractservice.doSuperSubmitKam(tkam.getKamid(), userid, groupid,
				roleid, remark);
	}

	public void drop(TKam tkam, Long userid, Long groupid, Long roleid,
			String remark) throws Exception {
		tkam.setStatus(11l);
		getEntityManager().update(tkam);
		TFlowOperateLogCM log = new TFlowOperateLogCM();
		log.setME_operatedate(new Date());
		log.setME_operatorid(userid);
		log.setME_remark(get(TPriGroupCM.class, groupid).getME_groupname()
				+ "的" + get(TPriRoleCM.class, roleid).getME_rolename() + ": "
				+ remark);
		log.setFlowtype(0l);
		log.setFlowtypeId(tkam.getKamid());
		log.setME_operatetype(3l);
		getEntityManager().save(log);
		contractservice.doSuperSubmitKam(tkam.getKamid(), userid, groupid,
				roleid, remark);
	}

	public void updateReaded(Long kamid) {
		TKam tkam = this.getTKamById(kamid);
		tkam.setReaded("yes");
		getEntityManager().update(tkam);
	}

	// 复制信息表
	@SuppressWarnings("unchecked")
	public TKamImage copyTKam(Long kamid, Long contractid)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		TKam tkam = getEntityManager().get(TKam.class, kamid);
		tkam.getTPriDept();
		TKamImage tkamImage = new TKamImage();
		PropertyUtils.copyProperties(tkamImage, tkam);
		tkamImage.setActivenodeid(contractid);
		tkamImage.setKamid(null);
		//tkamImage.setBonusdesc(contractservice.getManagerByKamId(kamid).getME_namecn());
		tkamImage.setKamlevel(tkam.getTPriDept().getDeptname());
		getEntityManager().save(tkamImage);
		hibernate.flush();
		// 主营业务
		String sqlBizType = "select map.BIZTYPEID from T_KAM_BIZ_MAP map where map.KAMID="
				+ kamid;
		List biztypeid = getEntityManager().queryForListBySql(sqlBizType);
		for (int j = 0; j < biztypeid.size(); j++) {
			String sql = "INSERT INTO T_KAM_BIZ_MAP(KAMID,BIZTYPEID) VALUES('"
					+ tkamImage.getKamid() + "','"
					+ biztypeid.get(j).toString() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		// 代理人
		String sqlAgent = "select DEPARTMENTID from T_KAM_AGENT_MAP map where map.KAMID="
				+ kamid;
		List agentid = getEntityManager().queryForListBySql(sqlAgent);
		for (int j = 0; j < agentid.size(); j++) {
			String sql = "INSERT INTO T_KAM_AGENT_MAP(KAMID,DEPARTMENTID) VALUES('"
					+ tkamImage.getKamid()
					+ "','"
					+ agentid.get(j).toString()
					+ "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		// 代理人的联系人
		String sqlContact = "select CONTACTID from T_Kam_Contact_Map map where map.KAMID="
				+ kamid;
		List contactid = getEntityManager().queryForListBySql(sqlContact);
		for (int j = 0; j < contactid.size(); j++) {
			String sql="select  min(map.kamid) from T_Kam_Contact_Map map";
			Integer kamidforcopy=(getEntityManager().queryForIntegerBySql(sql)-1);
			 sql = "INSERT INTO T_Kam_Contact_Map(KAMID,CONTACTID,IMAGEID) VALUES('"
					+ kamidforcopy
					+ "','"
					+ contactid.get(j).toString()
					+ "','"
					+ tkamImage.getKamid() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}

		// 关键人&&联系人
		String sqlcustomer = "select CUSTOMERID from T_KAM_CNTL_MAP map where map.KAMID="
				+ kamid;
		List customerid = getEntityManager().queryForListBySql(sqlcustomer);
		for (int j = 0; j < customerid.size(); j++) {
			String sql = "INSERT INTO T_KAM_CNTL_MAP(KAMID,CUSTOMERID) VALUES('"
					+ tkamImage.getKamid()
					+ "','"
					+ customerid.get(j).toString() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		// 操作日至
		String sqllog = "select log.KAMLOGID from t_kam_log  log where log.KAMID="
				+ kamid;
		List logid = getEntityManager().queryForListBySql(sqllog);
		for (int j = 0; j < logid.size(); j++) {
			String sql = "update t_kam_log set KAMID=-1,imageid="
					+ tkamImage.getKamid() + " where KAMID=" + kamid;
			getEntityManager().executeUpdateBySql(sql);
		}
		// 奖励政策
//		String sqlBasispolicyMap = "update T_KAM_BASISPOLICY_MAP map set map.kamid=-1,map.imageid='"
//				+ tkamImage.getKamid() + "' where map.CONTRACTID=" + contractid;
//		getEntityManager().executeUpdateBySql(sqlBasispolicyMap);
		return tkamImage;
	}

	// 复制的TKAM显示
	@SuppressWarnings("unchecked")
	public TKamImage doQueryTKamImage(Long contractid) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamImage.class);
		bqc.addExpressions(Restrictions.equal("activenodeid", contractid));
		List<TKamImage> tkamImages = getEntityManager().query(bqc);
		if (tkamImages.size() - 0 == 0)
			return null;
		else
			return tkamImages.get(0);

	}
	//查询大客户对应的增值服务，预更新或更新后显示其对应的增值服务时调用。
	@SuppressWarnings("unchecked")
	public List<ValueAddedService> queryValueByKam(Long kamid) {
		String sql = "select t.serviceid from kam_value_added_service_map t where t.kamid="
				+ kamid ;
		List<Object> valuelist = getEntityManager().queryForListBySql(sql);
		ArrayList<Long> templist = new ArrayList<Long>();
		if (valuelist != null && valuelist.size() > 0) {
			for (Object id : valuelist) {
				templist.add(Long.valueOf(id.toString()));
			}
		}
		if (templist != null && templist.size() > 0) {
			BeanQueryCondition bqc = BeanQueryCondition.forClass(ValueAddedService.class);
			bqc.addExpressions(Restrictions.in("serviceid", templist));
			List<ValueAddedService> list = getEntityManager().query(bqc);
			
			for (int i = 0; i < list.size(); i++) {
				String hql = "select c.TInfoCabin.cabinname from ValiableAddedserviceCabin c where c.valueAddedService.serviceid="
						+ list.get(i).getServiceid();
				List<String> cabins = getEntityManager().queryForListByHql(hql);
				String str = "";
				for (int j = 0; j < cabins.size(); j++) {
					str += cabins.get(j) + "/";
				}
				if (str.length() != 0) {
					str = str.substring(0, str.length() - 1);
					list.get(i).setCabinvaliable(str);
				}
				hql = "select c.TInfoCabin.cabinname from InvaliableAddedserviceCabin c where c.valueAddedService.serviceid="
						+ list.get(i).getServiceid();
				cabins = getEntityManager().queryForListByHql(hql);
				str = "";
				for (int j = 0; j < cabins.size(); j++) {
					str += cabins.get(j) + "/";
				}
				if (str.length() != 0) {
					str = str.substring(0, str.length() - 1);
					list.get(i).setCabininvaliable(str);
				}
			}
			return list;
		}
		return null;
	}
	
	//1.新增信息表时，大客户信息插到CallCenter
//	public Boolean doInsertTKamToCC(TKam tkam,List<TSpecialAirline> specialAirlines){
//		try{		
////		GroupManagement_Service client = new GroupManagement_Service();
////		GroupManagement services = client.getServicePort();	
//			KmsGroupSyncImplService client = new KmsGroupSyncImplService();
//			KmsGroupSync services = client.getKmsGroupSyncImplPort();
//		com.cea.callcenter.ws.kms.ObjectFactory ob = new com.cea.callcenter.ws.kms.ObjectFactory();	
//		com.cea.callcenter.ws.kms.GroupKamInfo groupKamInfo = ob.createGroupKamInfo();
//		if(tkam!=null){		
//			if(tkam.getAgreementtype()!=null){
//				groupKamInfo.setAgreementtype(BigDecimal.valueOf(tkam.getAgreementtype()));	//协议类型	
//			}
//			if(tkam.getAgreementstatus()!=null){
//				groupKamInfo.setAgreementstatus(BigDecimal.valueOf(tkam.getAgreementstatus()));	//协议状态		
//			}
//			if(tkam.getBalancetype()!=null){
//				groupKamInfo.setBalancetype(BigDecimal.valueOf(tkam.getBalancetype()));	//结算类型		
//			}
//			groupKamInfo.setBonusdesc(tkam.getBonusdesc());	
//			if(tkam.getBonuspercent()!=null){
//				groupKamInfo.setBonuspercent(BigDecimal.valueOf(tkam.getBonuspercent()));			
//			}
//			if(tkam.getCompanytype()!=null){	
//				groupKamInfo.setCompanytype(BigDecimal.valueOf(tkam.getCompanytype())); //公司类型
//			}
//			if(tkam.getDelflag()!=null){
//				groupKamInfo.setDelflag(BigDecimal.valueOf(tkam.getDelflag()));			//是否有效
//			}
//			if(tkam.getEmployeenumber()!=null){
//				groupKamInfo.setEmployeenumber(BigDecimal.valueOf(tkam.getEmployeenumber()));//员工数量	 		
//			}
//			if(tkam.getExpditurelmt()!=null){
//				groupKamInfo.setExpditurelmt(Double.valueOf(tkam.getExpditurelmt()));
//			}
//			if(tkam.getKamcontinue()!=null){
//				groupKamInfo.setFlowid(BigDecimal.valueOf(tkam.getKamcontinue()));			
//			}		
//			if(tkam.getKamcontinue()!=null){
//				groupKamInfo.setKamcategory(BigDecimal.valueOf(tkam.getKamcontinue()));			
//			}
//			if(tkam.getKamcontinue()!=null){
//				groupKamInfo.setKamcontinue(BigDecimal.valueOf(tkam.getKamcontinue()));			
//			}
//			if(tkam.getRestcapital()!=null){
//				groupKamInfo.setRestcapital(BigDecimal.valueOf(tkam.getRestcapital()));			
//			}
//			if(tkam.getStatus()!=null){
//				groupKamInfo.setStatus(BigDecimal.valueOf(tkam.getStatus()));		
//			}
//			if(tkam.getTravelcost()!=null){
//				groupKamInfo.setTravelcost(Double.valueOf(tkam.getTravelcost()));		
//			}
//			if(tkam.getKamrank()!=null){
//				groupKamInfo.setKamrank(BigDecimal.valueOf(tkam.getKamrank()));			
//			}
//			if(tkam.getKamrelationship()!=null){
//				groupKamInfo.setKamrelationship(BigDecimal.valueOf(tkam.getKamrelationship()));			
//			}
//			if(tkam.getKamtype()!=null){
//				groupKamInfo.setKamtype(BigDecimal.valueOf(tkam.getKamtype()));			
//			}
//			if(tkam.getLasttaxamount()!=null){
//				groupKamInfo.setLasttaxamount(BigDecimal.valueOf(tkam.getLasttaxamount()));		
//			}
//			if(tkam.getLeafgruop()!=null){
//				groupKamInfo.setLeafgruop(BigDecimal.valueOf(tkam.getLeafgruop()));			
//			}
//			if(tkam.getKamid()!=null){
//				groupKamInfo.setKamid(BigDecimal.valueOf(tkam.getKamid()));				
//			}
//			groupKamInfo.setAddress(tkam.getAddress());
//			groupKamInfo.setFax(tkam.getFax());
//			groupKamInfo.setCompanytypestr(tkam.getCompanytypestr());
//
//			groupKamInfo.setKamlevel(tkam.getKamlevel());
//			groupKamInfo.setKamnamecn(tkam.getKamnamecn());
//			groupKamInfo.setKamnameen(tkam.getKamnameen());
//			groupKamInfo.setKamno(tkam.getKamno());
//			groupKamInfo.setMuapydeptnote(tkam.getMuapydeptnote());
//			groupKamInfo.setMucrmdeptnotion(tkam.getMucrmdeptnotion());
//			groupKamInfo.setMurvudeptnotion(tkam.getMurvudeptnotion());
//			groupKamInfo.setPostcode(tkam.getPostcode());
//			
//			//拼接国际特殊航线备注到备注
//			String remark = tkam.getRemark();
//			if("all".equals(tkam.getRouteType())){
//				if(remark != null && !"".equals(remark)){
//					remark += "/n";
//				}
//				remark += "国际所有航线全舱折扣/n";
//				
//			}else if("some".equals(tkam.getRouteType())){
//				if(remark != null && !"".equals(remark)){
//					remark += "/n";
//				}
//				remark += "国际指定航线折扣/n";
//				if(specialAirlines != null){
//					for (int i = 0; i < specialAirlines.size(); i++) {
//						TSpecialAirline specialAirline = specialAirlines.get(i);
//						remark += "航线：" + specialAirline.getAirlineStart() + specialAirline.getAirlineEnd() + ";";
//						remark += "舱位折扣：";
//						List<TSpaceDiscount> spaceDiscounts = specialAirline.getTSpaceDiscounts();
//						for (int j = 0; j < spaceDiscounts.size(); j++) {
//							TSpaceDiscount spaceDiscount = spaceDiscounts.get(j);
//							if("true".equals(spaceDiscount.getSpaceType())){
//								remark += spaceMap.get(j) + spaceDiscount.getSpaceDiscount()+",";
//							}
//						}
//						remark += ";";
//						remark += "单程/回程：" + specialAirline.getRangeType() + ";/n";
//					}
//				}
//			}
//			groupKamInfo.setRemark(remark);
//			
//			groupKamInfo.setTravellocation(tkam.getTravellocation());
//			groupKamInfo.setWebsite(tkam.getWebsite());
//			//groupKamInfo.setRegistdate();
//			//groupKamInfo.setRejectdate();
//			//groupKamInfo.setOperatedate();
//			//groupKamInfo.setOperateid(BigDecimal.valueOf(tkam.getOperateid()));
//			//groupKamInfo.setDepartmentid();
//			//groupKamInfo.setGroupid(BigDecimal.valueOf(tkam.getGroupid()));
//			//groupKamInfo.setCreatedate();
//			//groupKamInfo.setLastsigndate();
//			//groupKamInfo.setCreatorid(BigDecimal.valueOf(tkam.getCreatorid()));		
//			services.insertGroupKam(groupKamInfo);	
//			return true;
//		}
//		} catch(Exception e ){
//			e.printStackTrace();
//			if(e != null && e.toString().length()>3999)
//			logfaild.setRemark(e.toString().substring(0,3999));
//		}
//		return false;
//	}
	//2.TKam对应的关键人写到CallCenter
//	public Boolean doInsertCustomerToCC(TKam tkam,TKamCustomer tkamCustomer){
//		try{
////		GroupManagement_Service client = new GroupManagement_Service();
////		GroupManagement services = client.getServicePort();
//			KmsGroupSyncImplService client = new KmsGroupSyncImplService();
//			KmsGroupSync services = client.getKmsGroupSyncImplPort();
//		com.cea.callcenter.ws.kms.ObjectFactory ob = new com.cea.callcenter.ws.kms.ObjectFactory();	
//		if (tkamCustomer != null) {
//				//员工表数据写入
//			com.cea.callcenter.ws.kms.GroupMemberInfo groupMemberInfo = ob.createGroupMemberInfo();					
//				groupMemberInfo.setAddress(tkamCustomer.getAddress());	
//				groupMemberInfo.setBirth(tkamCustomer.getBirth());
//				groupMemberInfo.setCustomernumber(tkamCustomer.getCustomernumber());
//				groupMemberInfo.setDepartment(tkamCustomer.getDepartment());
//				groupMemberInfo.setEmail(tkamCustomer.getEmail());
//				groupMemberInfo.setEmployno(tkamCustomer.getEmployno());
//				groupMemberInfo.setFax(tkamCustomer.getFax());
//				groupMemberInfo.setMembercard(tkamCustomer.getMembercard());
//				groupMemberInfo.setMembercode(tkamCustomer.getMembercode());
//				groupMemberInfo.setMobile(tkamCustomer.getMobile());
//				groupMemberInfo.setNamecn(tkamCustomer.getNamecn());
//				groupMemberInfo.setNameen(tkamCustomer.getNameen());
//				groupMemberInfo.setNationality(tkamCustomer.getNationality());
//				groupMemberInfo.setOthercode(tkamCustomer.getOthercode());
//				groupMemberInfo.setPassportcode(tkamCustomer.getPassportcode());
//				groupMemberInfo.setPosition(tkamCustomer.getPosition());
//				groupMemberInfo.setPostcode(tkamCustomer.getPostcode());
//				groupMemberInfo.setRemark(tkamCustomer.getRemark());
//				groupMemberInfo.setResidence(tkamCustomer.getResidence());
//				groupMemberInfo.setTelephone(tkamCustomer.getTelephone());
//				groupMemberInfo.setTelephone2(tkamCustomer.getTelephone2());
//				groupMemberInfo.setTelephone3(tkamCustomer.getTelephone3());
//				groupMemberInfo.setTelephone4(tkamCustomer.getTelephone4());
//				groupMemberInfo.setTelephone5(tkamCustomer.getTelephone5());		
//				services.insertGroupMember(groupMemberInfo);
//				//中间表数据写入
//				com.cea.callcenter.ws.kms.GroupMapInfo groupGroupMapInfo = ob.createGroupMapInfo();
//				if(tkamCustomer.getCustomerid()!=null){
//					groupGroupMapInfo.setCustomerid(BigDecimal.valueOf(tkamCustomer.getCustomerid()));				
//				}
//				if(tkam.getKamid()!=null){
//					groupGroupMapInfo.setKamid(BigDecimal.valueOf(tkam.getKamid()));					
//				}
//				services.insertGroupMap(groupGroupMapInfo);
//				return true;
//		}
//		}catch(Exception e ){
//			e.printStackTrace();
//		}
//			return false ;
//	}
	//1.更新信息表时，大客户信息同步数据到CallCenter
	public Boolean doUpdatetTKamToCC(TKam tkam,List<TSpecialAirline> specialAirlines){
		try{		
//		GroupManagement_Service client = new GroupManagement_Service();
//		GroupManagement services = client.getServicePort();	
			KmsGroupSyncImplService client = new KmsGroupSyncImplService();
			KmsGroupSync services = client.getKmsGroupSyncImplPort();
		com.cea.callcenter.ws.kms.ObjectFactory ob = new com.cea.callcenter.ws.kms.ObjectFactory();	
		com.cea.callcenter.ws.kms.GroupKamInfo groupKamInfo = ob.createGroupKamInfo();
		if(tkam!=null){		
			if(tkam.getAgreementtype()!=null){
				groupKamInfo.setAgreementtype(BigDecimal.valueOf(tkam.getAgreementtype()));	//协议类型	
			}
			if(tkam.getAgreementstatus()!=null){
				groupKamInfo.setAgreementstatus(BigDecimal.valueOf(tkam.getAgreementstatus()));	//协议状态		
			}
			if(tkam.getBalancetype()!=null){
				groupKamInfo.setBalancetype(BigDecimal.valueOf(tkam.getBalancetype()));	//结算类型		
			}
			groupKamInfo.setBonusdesc(tkam.getBonusdesc());	
			if(tkam.getBonuspercent()!=null){
				groupKamInfo.setBonuspercent(BigDecimal.valueOf(tkam.getBonuspercent()));			
			}
			if(tkam.getCompanytype()!=null){	
				groupKamInfo.setCompanytype(BigDecimal.valueOf(tkam.getCompanytype())); //公司类型
			}
			if(tkam.getDelflag()!=null){
				groupKamInfo.setDelflag(BigDecimal.valueOf(tkam.getDelflag()));			//是否有效
			}
			if(tkam.getEmployeenumber()!=null){
				groupKamInfo.setEmployeenumber(BigDecimal.valueOf(tkam.getEmployeenumber()));//员工数量	 		
			}
			if(tkam.getExpditurelmt()!=null){
				groupKamInfo.setExpditurelmt(Double.valueOf(tkam.getExpditurelmt()));
			}
			if(tkam.getKamcontinue()!=null){
				groupKamInfo.setFlowid(BigDecimal.valueOf(tkam.getKamcontinue()));			
			}		
			if(tkam.getKamcontinue()!=null){
				groupKamInfo.setKamcategory(BigDecimal.valueOf(tkam.getKamcontinue()));			
			}
			if(tkam.getKamcontinue()!=null){
				groupKamInfo.setKamcontinue(BigDecimal.valueOf(tkam.getKamcontinue()));			
			}
			if(tkam.getRestcapital()!=null){
				groupKamInfo.setRestcapital(BigDecimal.valueOf(tkam.getRestcapital()));			
			}
			if(tkam.getStatus()!=null){
				groupKamInfo.setStatus(BigDecimal.valueOf(tkam.getStatus()));		
			}
			if(tkam.getTravelcost()!=null){
				groupKamInfo.setTravelcost(Double.valueOf(tkam.getTravelcost()));		
			}
			if(tkam.getKamrank()!=null){
				groupKamInfo.setKamrank(BigDecimal.valueOf(tkam.getKamrank()));			
			}
			if(tkam.getKamrelationship()!=null){
				groupKamInfo.setKamrelationship(BigDecimal.valueOf(tkam.getKamrelationship()));			
			}
			if(tkam.getKamtype()!=null){
				groupKamInfo.setKamtype(BigDecimal.valueOf(tkam.getKamtype()));			
			}
			if(tkam.getLasttaxamount()!=null){
				groupKamInfo.setLasttaxamount(BigDecimal.valueOf(tkam.getLasttaxamount()));		
			}
			if(tkam.getLeafgruop()!=null){
				groupKamInfo.setLeafgruop(BigDecimal.valueOf(tkam.getLeafgruop()));			
			}
			if(tkam.getKamid()!=null){
				groupKamInfo.setKamid(BigDecimal.valueOf(tkam.getKamid()));				
			}
			groupKamInfo.setAddress(tkam.getAddress());
			groupKamInfo.setFax(tkam.getFax());
			groupKamInfo.setCompanytypestr(tkam.getCompanytypestr());
			groupKamInfo.setKamlevel(tkam.getKamlevel());
			groupKamInfo.setKamnamecn(tkam.getKamnamecn());
			groupKamInfo.setKamnameen(tkam.getKamnameen());
			groupKamInfo.setKamno(tkam.getKamno());
			groupKamInfo.setMuapydeptnote(tkam.getMuapydeptnote());
			groupKamInfo.setMucrmdeptnotion(tkam.getMucrmdeptnotion());
			groupKamInfo.setMurvudeptnotion(tkam.getMurvudeptnotion());
			groupKamInfo.setPostcode(tkam.getPostcode());
			
			//拼接国际特殊航线备注到备注
			String remark = tkam.getRemark();
			if("all".equals(tkam.getRouteType())){
				if(remark != null && !"".equals(remark)){
					remark += "\n";
				}
				remark += "国际所有航线全舱折扣\n";
				
			}else if("some".equals(tkam.getRouteType())){
				if(remark != null && !"".equals(remark)){
					remark += "\n";
				}
				remark += "国际指定航线折扣\n";
				if(specialAirlines != null){
					for (int i = 0; i < specialAirlines.size(); i++) {
						TSpecialAirline specialAirline = specialAirlines.get(i);
						remark += "航线：" + specialAirline.getAirlineStart() +"-"+ specialAirline.getAirlineEnd() + ";";
						remark += "舱位折扣：";
						List<TSpaceDiscount> spaceDiscounts = specialAirline.getTSpaceDiscounts();
						for (int j = 0; j < spaceDiscounts.size(); j++) {
							TSpaceDiscount spaceDiscount = spaceDiscounts.get(j);
							if("true".equals(spaceDiscount.getSpaceType())){
								remark += spaceMap.get(j) + spaceDiscount.getSpaceDiscount()+",";
							}
						}
						remark += ";";
						remark += "单程/回程：" + specialAirline.getRangeType() + ";\n";
					}
				}
			}
			groupKamInfo.setRemark(remark);
			
			groupKamInfo.setTravellocation(tkam.getTravellocation());
			groupKamInfo.setWebsite(tkam.getWebsite());
			//groupKamInfo.setRegistdate();
			//groupKamInfo.setRejectdate();
			//groupKamInfo.setOperatedate();
			//groupKamInfo.setOperateid(BigDecimal.valueOf(tkam.getOperateid()));
			//groupKamInfo.setDepartmentid();
			//groupKamInfo.setGroupid(BigDecimal.valueOf(tkam.getGroupid()));
			//groupKamInfo.setCreatedate();
			//groupKamInfo.setLastsigndate();
			//groupKamInfo.setCreatorid(BigDecimal.valueOf(tkam.getCreatorid()));		
			services.updateGroupKam(groupKamInfo);	
			return true;
			}
		} catch(Exception e ){
			e.printStackTrace();
		}
		return false;
	}
	//2.TKam对应的关键人或者联系人写到CallCenter
//	public Boolean doUpdateCustomerToCC(TKam tkam,TKamCustomer tkamCustomer){
//		try{
//		if (tkamCustomer != null) {
////				GroupManagement_Service client = new GroupManagement_Service();
////				GroupManagement services = client.getServicePort();
//			KmsGroupSyncImplService client = new KmsGroupSyncImplService();
//				KmsGroupSync services = client.getKmsGroupSyncImplPort();
//				com.cea.callcenter.ws.kms.ObjectFactory ob = new com.cea.callcenter.ws.kms.ObjectFactory();
//				//员工表数据写入
//				com.cea.callcenter.ws.kms.GroupMemberInfo groupMemberInfo = ob.createGroupMemberInfo();
//				if(tkamCustomer.getCompanyid()!=null){
//					groupMemberInfo.setCompanyid(BigDecimal.valueOf(tkamCustomer.getCompanyid()));					
//				}
//				if(tkamCustomer.getCustomerid()!=null){
//					groupMemberInfo.setCustomerid(BigDecimal.valueOf(tkamCustomer.getCustomerid()));					
//				}
//				if(tkamCustomer.getCustomertype()!=null){
//					groupMemberInfo.setCustomertype(BigDecimal.valueOf(tkamCustomer.getCustomertype()));					
//				}
//				if(tkamCustomer.getDelflag()!=null){
//					groupMemberInfo.setDelflag(BigDecimal.valueOf(tkamCustomer.getDelflag()));				
//				}
//				if(tkamCustomer.getMemberlevel()!=null){
//					groupMemberInfo.setMemberlevel(BigDecimal.valueOf(tkamCustomer.getMemberlevel()));					
//				}
//				if(tkamCustomer.getOperatorid()!=null){
//					groupMemberInfo.setOperatorid(BigDecimal.valueOf(tkamCustomer.getOperatorid()));				
//				}
//				if(tkamCustomer.getServicelevel()!=null){
//					groupMemberInfo.setServicelevel(BigDecimal.valueOf(tkamCustomer.getServicelevel()));			
//				}
//				if(tkamCustomer.getSex()!=null){
//					groupMemberInfo.setSex(BigDecimal.valueOf(tkamCustomer.getSex()));				
//				}	
//				groupMemberInfo.setIdcardno(tkamCustomer.getIdcardno());
//				groupMemberInfo.setAddress(tkamCustomer.getAddress());	
//				groupMemberInfo.setBirth(tkamCustomer.getBirth());
//				groupMemberInfo.setCustomernumber(tkamCustomer.getCustomernumber());
//				groupMemberInfo.setDepartment(tkamCustomer.getDepartment());
//				groupMemberInfo.setEmail(tkamCustomer.getEmail());
//				groupMemberInfo.setEmployno(tkamCustomer.getEmployno());
//				groupMemberInfo.setFax(tkamCustomer.getFax());
//				groupMemberInfo.setMembercard(tkamCustomer.getMembercard());
//				groupMemberInfo.setMembercode(tkamCustomer.getMembercode());
//				groupMemberInfo.setMobile(tkamCustomer.getMobile());
//				groupMemberInfo.setNamecn(tkamCustomer.getNamecn());
//				groupMemberInfo.setNameen(tkamCustomer.getNameen());
//				groupMemberInfo.setNationality(tkamCustomer.getNationality());
//				groupMemberInfo.setOthercode(tkamCustomer.getOthercode());
//				groupMemberInfo.setPassportcode(tkamCustomer.getPassportcode());
//				groupMemberInfo.setPosition(tkamCustomer.getPosition());
//				groupMemberInfo.setPostcode(tkamCustomer.getPostcode());
//				groupMemberInfo.setRemark(tkamCustomer.getRemark());
//				groupMemberInfo.setResidence(tkamCustomer.getResidence());
//				groupMemberInfo.setTelephone(tkamCustomer.getTelephone());
//				groupMemberInfo.setTelephone2(tkamCustomer.getTelephone2());
//				groupMemberInfo.setTelephone3(tkamCustomer.getTelephone3());
//				groupMemberInfo.setTelephone4(tkamCustomer.getTelephone4());
//				groupMemberInfo.setTelephone5(tkamCustomer.getTelephone5());
////				groupMemberInfo.setPsprtvalidate();
////				groupMemberInfo.setOperatedate();
//				Boolean result = services.updateGroupMember(groupMemberInfo);
//				if(result){
//					logger.info(new Date() +":" + groupMemberInfo.getCompanyid() + ":"+ "成功");
//				}
//				if(!result){
//					logger.error(new Date() +":" + groupMemberInfo.getCompanyid() + ":"+ "失败");
//				}
//				//中间表数据写入
//				com.cea.callcenter.ws.kms.GroupMapInfo groupGroupMapInfo = ob.createGroupMapInfo();
//				if(tkamCustomer.getCustomerid()!=null){
//					groupGroupMapInfo.setCustomerid(BigDecimal.valueOf(tkamCustomer.getCustomerid()));					
//				}
//				if(tkam.getKamid()!=null){
//					groupGroupMapInfo.setKamid(BigDecimal.valueOf(tkam.getKamid()));					
//				}
//				services.updateGroupMap(groupGroupMapInfo);
//				return true;
//		}
//		} catch(Exception e){
//			e.printStackTrace();
//		}
//		return false;
//	}
	@SuppressWarnings("unchecked")
	public TKam getKamidByKamno(String kamno) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		bqc.addExpressions(Restrictions.equal("kamno", kamno));
		List<TKam> tkam = getEntityManager().query(bqc);
		if(tkam!=null && tkam.size()>0){
			return tkam.get(0);
		}
		return null;
	}
	@SuppressWarnings("unchecked")
	public TKam getKamByKamno(String kamno) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		bqc.addExpressions(Restrictions.equal("kamno", kamno));
		bqc.addExpressions(Restrictions.isNotNull("agreementtype"));
		List<TKam> tkam = getEntityManager().query(bqc);
		if(tkam!=null && tkam.size()>0){
			return tkam.get(0);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public TKamCM getKamCMByKamno(String kamno) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamCM.class);
		bqc.addExpressions(Restrictions.equal("kamno", kamno));
		bqc.addExpressions(Restrictions.isNotNull("agreementtype"));
		List<TKamCM> tkam = getEntityManager().query(bqc);
		if(tkam!=null && tkam.size()>0){
			return tkam.get(0);
		}
		return null;
	}	
	
	//查询b2g中的comp
	@SuppressWarnings("unchecked")
	public TrCompVO getTrCompByKamno(String kamno) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TrCompVO.class);
		bqc.addExpressions(Restrictions.equal("kamno", kamno));
		List<TrCompVO> trComp = getB2bEntityManager().query(bqc);
		if(trComp!=null && trComp.size()>0){
			return trComp.get(0);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public List<TKam> getRepeatTKamNO(OrderablePagination p){
		String sql = "select t.kamno from t_kam t " +
				"where t.delflag<>1 and length(t.kamno)>5 and t.status>2" +
				"group by t.kamno " +
				"having count (substr(t.kamno,1,6))>1";
		List result = getEntityManager().queryForListBySql(sql);
		
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		if(result != null && result.size()>0)
			bqc.addExpressions(Restrictions.in("kamno", result));
		else 
			bqc.addExpressions(Restrictions.equal("kamno", "AAA"));
		bqc.addExpressions(Restrictions.equal("delflag", new Long(1)));
		bqc.addInitFields("TPriDept");
		Sorter[] sorters = p.getSorters(); 
		if (ObjectUtils.isEmpty(sorters)) {
			bqc.addSorters(Sorter.desc("kamno"));
		} else {
			bqc.addSorters(sorters);
		}
		List<TKam> tkam = getEntityManager().queryWithPagination(bqc,p);
		return tkam;
	}
	@SuppressWarnings("unchecked")
	public List<TKam> getTKamWithoutContract(OrderablePagination p){
		String hql = "from TKam t where t.delflag=0 and t.status=7 and  not exists " +
				"(select c.TKam.kamid from TKamContract c where c.delflag=0 and c.TKam.kamid=t.kamid)";
		List result = getEntityManager().queryForListByHql(hql, p);
		return result;
	}
	
	/**
	 * 导出近期已审核信息表
	 */
	@SuppressWarnings("unchecked")
	public List<TKam> queryForDownload(TKamDto tkamDto, Long groupid, OrderablePagination p) throws ServiceException {
		//查询的sql
		StringBuffer hql = queryTKamApprovedSql(tkamDto, p, groupid);
		if(hql == null) {
			return new ArrayList<TKam>();
		}
		List<Object[]> objs = getEntityManager().queryForListBySql(hql.toString());
		
		List<TKam> kams = new ArrayList<TKam>();
		TKam kam ;
		Object[] obj;
		for(int i = 0; i < objs.size(); i++) {
			kam = new TKam();
			obj = objs.get(i);
			kam.setKamid(((BigDecimal)obj[0]).longValue());
			kam.setKamno((String)obj[1]);
			kam.setKamnamecn((String)obj[2]);
			kam.setKamnameen((String)obj[3]);
//			if(obj[4] != null)
//				kam.setCompanytype(((BigDecimal)obj[4]).longValue());
			if(obj[4] != null)
				kam.setStatus(((BigDecimal)obj[4]).longValue());
//			kam.setOperatorName((String)obj[6]);
			kam.setDeptName((String)obj[5]);
			kam.setLogOperatedate((Date)obj[6]);
//			kam.setCompanytypestr((String)obj[7]);
			kam.setValiDate((Date)obj[7]);
			kam.setInValiDate((Date)obj[8]);
			kams.add(kam);
		}
		return kams;

	}

	// 当前人有几张KAM带审批
	@SuppressWarnings("unchecked")
	public List<TKam> getKamToApprove(List<String> groupandrole) {
		StringBuffer grbuffer=new StringBuffer("(");
		for(int i=0;i<groupandrole.size();i++){
			grbuffer.append("'").append(groupandrole.get(i)).append("',");
		} 
		String gr= grbuffer.toString();
		gr=gr.substring(0,gr.length()-1);
		gr+=")";
		String sql = "select kam.kamid from t_kam kam ,t_flow_node node  where  kam.delflag=0 and (kam.status=4 or kam.status=13 or kam.status=7)"
				+ " and  kam.activenodeid=node.flownodeid and node.groupid||'a'||node.roleid in "+gr;
		List list = getEntityManager().queryForListBySql(sql);
	
		List<Long> newlist = new ArrayList<Long>();
		
		for (Object id : list) {
			newlist.add(Long.parseLong(((BigDecimal) id).toString()));
		}
		if (newlist != null && newlist.size() > 0) {
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
			bqc.addExpressions(Restrictions.in("kamid", newlist));
			List<TKam> result = getEntityManager().query(bqc);
			for (int i = 0; i < result.size(); i++) {
				TKam kam = result.get(i);
				TFlowNodeCM node=get(TFlowNodeCM.class, kam.getActivenodeid());
				String group=node.getTPriGroup().getME_groupname();
				String role=node.getTPriRole().getME_rolename();
				kam.setActivenodestr(group+"的"+role);
			}
			// 给TKam加载account以便显示"客户经理"一列
			for (int i = 0; i < result.size(); i++) {
				TKam kam = result.get(i);
				sql = "select company.ACCOUNTID from t_kam kam,t_kam_company company,t_kam_cmp_map kcmap where"
						+ " kam.kamid=kcmap.kamid and kcmap.companyid=company.companyid and kam.kamid="
						+ kam.getKamid();
				List managerid = getEntityManager().queryForListBySql(sql);
				if (managerid.size() != 0 && managerid.get(0) != null) {
					kam.setAccount(get(TPriAccountCM.class, new Long(managerid
							.get(0).toString())));
				} else {
					TPriAccountCM a = new TPriAccountCM();
					a.setME_namecn("暂无");
				}
			}
	//		for (int i = 0; i < result.size(); i++) {
	//			TKam kam = result.get(i);
	//			sql = "from TFlowOperateLogCM log where log.flowtype=0 and log.flowtypeId ="
	//					+ kam.getKamid()
	//					+ " and log.ME_operatetype=0 order by log.ME_operatedate desc";
	//			List<TFlowOperateLogCM> logs = getEntityManager()
	//					.queryForListByHql(sql);
	//			if (logs.size() != 0) {
	//				Date submitDate = ((TFlowOperateLogCM) getEntityManager()
	//						.queryForListByHql(sql).get(0)).getME_operatedate();
	//				kam.setSubmitDate(DateUtils.getFormatDate(submitDate,
	//						"yyyy-MM-dd"));
	//			}
	//		}
	//		for (int i = 0; i < result.size(); i++) {
	//			TKam kam = result.get(i);
	//			sql = "from TFlowOperateLogCM log where log.flowtype=0 and log.flowtypeId ="
	//					+ kam.getKamid()
	//					+ " and log.ME_operatetype=1 order by log.ME_operatedate desc";
	//			List<TFlowOperateLogCM> logs = getEntityManager()
	//					.queryForListByHql(sql);
	//			if (logs.size() != 0) {
	//				Date submitDate = ((TFlowOperateLogCM) getEntityManager()
	//						.queryForListByHql(sql).get(0)).getME_operatedate();
	//				kam.setPassDate(DateUtils.getFormatDate(submitDate,
	//						"yyyy-MM-dd"));
	//			}
	//		}
			return result;
	
		}
		return null;
	}
	
	// 当前人有几张KAM待审批(吴刚)
	@SuppressWarnings("unchecked")
	public List<TKam> getKamToApprove(List<String> groupandrole, Long accountid, String type) {	
		StringBuffer grbuffer=new StringBuffer("(");
		for(int i=0;i<groupandrole.size();i++){
			grbuffer.append("'").append(groupandrole.get(i)).append("',");
		} 
		String gr= grbuffer.toString();
		gr=gr.substring(0,gr.length()-1);
		gr+=")";
		String sql = "select kam.kamid from TKam kam ,TFlowNodeCM node  where  kam.delflag=0 and (kam.status=4 or kam.status=13)"
				+ " and  kam.activenodeid=node.ME_flownodeid and node.TPriGroup.ME_groupid||'a'||node.TPriRole.ME_roleid in "+gr;
//		List list = getEntityManager().queryForListBySql(sql);
		//吴刚
//		List list2 = new ArrayList();
//		if ("吴刚"
//				.equals(this.get(TPriAccountCM.class, accountid).getME_namecn())) {
//			sql = "select kam.kamid from t_kam kam where  kam.delflag=0 and kam.status=13";
//			list2 = getEntityManager().queryForListBySql(sql);
//		}
//		list.addAll(list2);
		
//		List<Long> newlist = new ArrayList<Long>();
		
//		for (Object id : list) {
//			newlist.add(Long.parseLong(((BigDecimal) id).toString()));
//		}
//		if (newlist != null && newlist.size() > 0) {
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
//			bqc.addExpressions(Restrictions.in("kamid", newlist));
			
			
			if ("吴刚"
					.equals(this.get(TPriAccountCM.class, accountid).getME_namecn()) ||
				"方雁云"
					.equals(this.get(TPriAccountCM.class, accountid).getME_namecn())	) {
				String sql2 = "select kam.kamid from TKam kam where  kam.delflag=0 and kam.status=13";
//				bqc.addExpressions(SuperRestrictions.in("kamid", sql2));
				bqc.addExpressions(Restrictions.or(SuperRestrictions.in("kamid", sql),SuperRestrictions.in("kamid", sql2)));
			}else{
				bqc.addExpressions(SuperRestrictions.in("kamid", sql));
			}
			
			List<TKam> result = getEntityManager().query(bqc);
			for (int i = 0; i < result.size(); i++) {
				TKam kam = result.get(i);
				TFlowNodeCM node=get(TFlowNodeCM.class, kam.getActivenodeid());
				String group=node.getTPriGroup().getME_groupname();
				String role=node.getTPriRole().getME_rolename();
				kam.setActivenodestr(group+"的"+role);
			}
			// 给TKam加载account以便显示"客户经理"一列
			for (int i = 0; i < result.size(); i++) {
				TKam kam = result.get(i);
				sql = "select company.ACCOUNTID from t_kam kam,t_kam_company company,t_kam_cmp_map kcmap where"
						+ " kam.kamid=kcmap.kamid and kcmap.companyid=company.companyid and kam.kamid="
						+ kam.getKamid();
				List managerid = getEntityManager().queryForListBySql(sql);
				if (managerid.size() != 0 && managerid.get(0) != null) {
					kam.setAccount(get(TPriAccountCM.class, new Long(managerid
							.get(0).toString())));
				} else {
					TPriAccountCM a = new TPriAccountCM();
					a.setME_namecn("暂无");
				}
			}
			return result;
//		}
//		return null;
	}
	
	@SuppressWarnings("unchecked")
	public List<TKamImage> queryTkamImagByKamId(String kamId) 
	{
		//1:根据kamId，获取kam对象
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		bqc.addExpressions( Restrictions.equal("kamid",Long.parseLong(kamId) ) );
		List<TKam> tkam=getEntityManager().query(bqc);
		if(tkam!=null && !tkam.isEmpty())
		{
			bqc = BeanQueryCondition.forClass(TKamImage.class);
			bqc.addExpressions( Restrictions.equal("kamno",tkam.get(0).getKamno()) );
			List<TKamImage> kams = getEntityManager().query(bqc);
			return kams;
		}
		else
		{
			return null;
		}
	}	
	
	//查询信息表对应的有效合同
	@SuppressWarnings("unchecked")
	public TKamContract getTKamContractByKamID(Long kamid){
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKamContract.class);
		bqc.addExpressions(Restrictions.and(Restrictions.equal("kamid", kamid),
				Restrictions.equal("delflag", 0L)
//				Restrictions.equal("contractstatus", 0L)//hchang 现在无需判断合同是否有效了
//				Restrictions.notEqual("policytype", "6")//非超额
				));
		bqc.addInitFields("TKam");
		List<TKamContract> result =  getEntityManager().query(bqc);
//		String hql = "from TKamContract t where t.TKam.kamid="+kamid+" and t.delflag=0 and t.contractstatus=0";
//		TKamContract result = (TKamContract)getEntityManager().queryForObjectByHql(hql);
		if(result!=null && result.size()>0){
			return result.get(0);	
		}
		else{
			return null;
		}
	}
	
	//返回上航东航的舱位对应map
	@SuppressWarnings("unchecked")
	public Map getClassMap() {
		Map classMap = new HashMap();
		classMap.put("F", "(-)");
		classMap.put("P", "(-)");
		classMap.put("C", "(C)");
		classMap.put("J", "(D)");
		classMap.put("O", "(J)");
		classMap.put("Y", "(Y)");
		classMap.put("K", "(U)");
		classMap.put("B", "(B)");
		classMap.put("E", "(L)");
		classMap.put("H", "(M)");
		classMap.put("L", "(T)");
		classMap.put("M", "(E)");
		classMap.put("N", "(H)");
		classMap.put("R", "(Q)");
		classMap.put("S", "(V)");
		classMap.put("V", "(W)");
		classMap.put("T", "(G)");
		classMap.put("W", "(K)");
		classMap.put("X", "(P)");
		classMap.put("G", "(G)");
		classMap.put("Q", "(Z)");
		return classMap;
	}	
	
	/**
	 * 根据KamId，获取有效的kamId
	 */
	@SuppressWarnings("unchecked")
	public TKam getValidKamidByKamNo(String kamno){
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TKam.class);
		bqc.addExpressions(Restrictions.equal("kamno", kamno));
		bqc.addExpressions(Restrictions.notEqual("delflag", 1L));
		List<TKam> tkam = getEntityManager().query(bqc);
		if(tkam!=null && tkam.size()>0){
			return tkam.get(0);
		}
		return null;
	}	
	//取得大客户对应的所有合同
	@SuppressWarnings("unchecked")
	public List<TKamContract> getTKamContractListByKamID(Long kamid){
		String hql = "from TKamContract t where t.TKam.kamid="+kamid;
		List<TKamContract> result = getEntityManager().queryForListByHql(hql);
		return result;
		
	}	
	
	// 复制信息表
	@SuppressWarnings("unchecked")
	public TKamImage copyTKamImage(TKam tkam, Long contractid)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		//copy属性
		TKamImage tkamImage = new TKamImage();
		PropertyUtils.copyProperties(tkamImage, tkam);
		//设置操作时刻对应的合同
		tkamImage.setActivenodeid(contractid);
		tkamImage.setKamid(null);
		
		TPriAccountCM manager = contractservice.getManagerByKamId(tkam.getKamid());
		if(manager != null && manager.getME_namecn() != null)
			tkamImage.setBonusdesc(manager.getME_namecn());
		//保存
		if(tkam.getTPriDept() != null && tkam.getTPriDept().getDeptname() != null)
			tkamImage.setKamlevel(tkam.getTPriDept().getDeptname());	
		
		getEntityManager().save(tkamImage);
		hibernate.flush();
		// 主营业务
		String sqlBizType = "select map.BIZTYPEID from T_KAM_BIZ_MAP map where map.KAMID="
				+ tkam.getKamid();
		List biztypeid = getEntityManager().queryForListBySql(sqlBizType);
		for (int j = 0; j < biztypeid.size(); j++) {
			String sql = "INSERT INTO T_KAM_BIZ_MAP(KAMID,BIZTYPEID) VALUES('"
					+ tkamImage.getKamid() + "','"
					+ biztypeid.get(j).toString() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		
		// 代理人
		String sqlAgent = "select DEPARTMENTID from T_KAM_AGENT_MAP map where map.KAMID="
				+ tkam.getKamid();
		List agentid = getEntityManager().queryForListBySql(sqlAgent);
		for (int j = 0; j < agentid.size(); j++) {
			String sql = "INSERT INTO T_KAM_AGENT_MAP(KAMID,DEPARTMENTID) VALUES('"
					+ tkamImage.getKamid()
					+ "','"
					+ agentid.get(j).toString()
					+ "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		
		// 代理人的联系人
		String sqlContact = "select CONTACTID from T_Kam_Contact_Map map where map.KAMID="
				+ tkam.getKamid();
		List contactid = getEntityManager().queryForListBySql(sqlContact);
		for (int j = 0; j < contactid.size(); j++) {
			String sql="select  min(map.kamid) from T_Kam_Contact_Map map";
			Integer kamidforcopy=(getEntityManager().queryForIntegerBySql(sql)-1);
			 sql = "INSERT INTO T_Kam_Contact_Map(KAMID,CONTACTID,IMAGEID) VALUES('"
					+ kamidforcopy
					+ "','"
					+ contactid.get(j).toString()
					+ "','"
					+ tkamImage.getKamid() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}

		// 关键人&&联系人
		String sqlcustomer = "select CUSTOMERID from T_KAM_CNTL_MAP map where map.KAMID="
				+ tkam.getKamid();
		List customerid = getEntityManager().queryForListBySql(sqlcustomer);
		for (int j = 0; j < customerid.size(); j++) {
			String sql = "INSERT INTO T_KAM_CNTL_MAP(KAMID,CUSTOMERID) VALUES('"
					+ tkamImage.getKamid()
					+ "','"
					+ customerid.get(j).toString() + "')";
			getEntityManager().executeUpdateBySql(sql);
		}
		
		// 操作日志
		String sql = "update t_kam_log set KAMID=-1,imageid="
				+ tkamImage.getKamid() + " where KAMID=" + tkam.getKamid();
		getEntityManager().executeUpdateBySql(sql);
		
		// 奖励政策
		String hqlpolicy = "from TUseContractAwardsMap where kamid = ?";
		List<TUseContractAwardsMap> result = this.getEntityManager().queryForListByHql(hqlpolicy, new Object[]{tkam.getKamid()});
		if(result != null && result.size() > 0){
			for(TUseContractAwardsMap m:result){
				TUseContractAwardsMap map = new TUseContractAwardsMap();
				map.setKamid(tkamImage.getKamid());
				map.setBonusid(m.getBonusid());
				map.setPolicytype(m.getPolicytype());
				map.setTKamContract(m.getTKamContract());
				this.doSave(map);
			}
		}
		return tkamImage;
	}   
	
	// 取得当前权限下可以得到营业部的信息
	@SuppressWarnings("unchecked")
	public List<TPriDeptCM> getDeptByGroup(Long groupid) throws ServiceException {
		String hql = "from TPriDeptCM dept where dept.ME_delflag=0 and dept.ME_depttype=0"
			+ " and exists ( select 'x' from TPriGroupCM group where group.ME_grouplevel=0 and group.ME_deptid=dept.ME_departmentid)";
		//所有权限
		List<Long> groupidList = contractservice.getAllgroupByGroup(groupid);
		hql = hql + " and dept.ME_departmentid in (select groupC.ME_deptid from TPriGroupCM groupC where groupC.ME_groupid in ("+groupidList.get(0);
		if(groupidList.size()>1){
			for(int i = 1; i < groupidList.size(); i++) {
				hql = hql + "," + groupidList.get(i);
			}
		}
		hql = hql + "))";
		List<TPriDeptCM> list = getEntityManager().queryForListByHql(hql);
		
		for (int i = 0; i < list.size(); i++) {
			Long groupId = ((TPriGroupCM) getEntityManager().queryForListByHql(
					"from TPriGroupCM group where group.ME_grouplevel=0 and group.ME_deptid="
							+ list.get(i).getME_departmentid()).get(0))
					.getME_groupid();
			list.get(i).setGroupId(groupId);
		}
		return list;
	}
	
	//获取营业部下面的客户经理
	@SuppressWarnings("unchecked")
	public List<TPriAccount> queryForAccountByDeptid(String deptId) {
		String hql = "select account from TPriAccount account, TpriRoleGroupAccountMap map where account.accountid = map.tpriAccountVO.accountId" +
				" and map.tpriRoleVO.roleName like '客户经理' and map.tpriGroupVO.deptId =" + deptId;
		return getEntityManager().queryForListByHql(hql);
	}
	
	//获取续签前的大客户消费额
	@SuppressWarnings("unchecked")
	public Map<String, String> queryBeforeTkamCost(String contractId, String kamid) {
		Map<String, String> map = new HashMap<String, String>();
		TKamContract contract = getEntityManager().get(TKamContract.class, Long.parseLong(contractId));
		Date today = this.getCurrentTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		String hql = null;
		//开始时间
		map.put("startDT", sdf.format(contract.getValidatedate()));
		if(today.after(contract.getInvalidatedate())) {
			hql = "select sum(t.psgIncome) from VKaSaleVO t where  t.saleMonth <= '" + sdf.format(contract.getInvalidatedate())+"'" 
			+ " and t.saleMonth >= '" + sdf.format(contract.getValidatedate()) + "' and t.kamId ="+kamid;
			//结束时间
			map.put("endDT", sdf.format(contract.getInvalidatedate()));
		}else {
			hql = "select sum(t.psgIncome) from VKaSaleVO t where  t.saleMonth <= '" + sdf.format(today)+"'" 
			+ " and t.saleMonth >= '" + sdf.format(contract.getValidatedate()) + "' and t.kamId ="+kamid;
			//结束时间
			map.put("endDT", sdf.format(today));
		}
		Double d = (Double)getEntityManager().queryForObjectByHql(hql);
		//消费额
		if(d != null) {
			DecimalFormat df = new DecimalFormat("#0.000");
			map.put("cost", df.format(d/10000.0));
			//与最低消费额与实际消费额的比率
			Float expd =  getEntityManager().get(TKam.class, Long.parseLong(kamid)).getExpditurelmt();
			if(expd != null)
				map.put("percent", df.format(d/(expd*10000)*100));
		}else {
			map.put("cost", "0");
			map.put("percent", "0");
		}
		return map;
	}
	
	//查询当前角色范围内的tkam数据
	@SuppressWarnings("unchecked")
	public List<TKam> queryTkamByGroup(Long groupId, Long roleId, Long accountId) throws ServiceException {
		
		List<TKam> list = new ArrayList<TKam>();
		StringBuffer sql = new StringBuffer(" select kam.kamid, kam.kamno, kam.kamnamecn, kam.kamnameen from t_kam kam " )
				.append(" where kam.delflag = 0 and kam.status > 2 and kam.status != 11 ");
		
		//按deptid查询大客户
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			for(int i = 0; i < deptList.size(); i++) {
				if(i == 0) 
					sql.append(" and kam.departmentid in ( " + deptList.get(i).getME_departmentid());
				else
					sql.append( " , " + deptList.get(i).getME_departmentid());
			}
			sql.append(") ");
		}else {
			return new ArrayList();
		}
		
		//如果是客户经理
		String managerName = contractservice.getManagerName(accountId, roleId, groupId);
		if(managerName != null) {
			List<Long> kamids = getKamByManagerId(managerName);
			if(kamids != null && kamids.size() != 0) {
				for(int i = 0; i < kamids.size(); i++) {
					if(i == 0)
						sql.append(" and kam.kamid in ( " + kamids.get(i));
					else
						sql.append(" , " + kamids.get(i));
				}
				sql.append(") ") ;
			}
		}
		sql.append(" order by kam.kamno ");
		List<Object[]> objs = getEntityManager().queryForListBySql(sql.toString());
		TKam kam = null;
		for(Object[] obj : objs) {
			if(obj[1] != null && obj[2] != null){
				kam = new TKam();
				kam.setKamid(((BigDecimal)obj[0]).longValue());
				kam.setKamno(obj[1].toString());
				kam.setKamnamecn(obj[2].toString());
				if(obj[3] != null)
					kam.setKamnameen(obj[3].toString());
				list.add(kam);
			}
		}
		return list;
	}
	
	//查询当前角色范围内的tkam数据
	@SuppressWarnings("unchecked")
	public List<TKam> queryTkamByGroup(TKamUatpReportDto dto,OrderablePagination p,Long groupId, Long roleId, Long accountId) throws ServiceException {
		
		List<TKam> list = new ArrayList<TKam>();
		StringBuffer sql = new StringBuffer(" select kam.kamid, kam.kamno, kam.kamnamecn, kam.kamnameen ,kam.btwogvisible from t_kam kam " )
				.append(" where kam.delflag = 0 and kam.status > 2 and kam.status != 11 ");
		
		if(!StringUtils.isBlank(dto.getKamno()))
			sql.append(" and kam.kamno = '"+dto.getKamno()+"' ");
		if(!StringUtils.isBlank(dto.getCardno()))
			sql.append(" and exists (select 1 from t_kam_customer_card card where kam.kamid = card.kamid and card.cardid='161' and card.cardnumber ='"+dto.getCardno()+"' ) ");
		if(dto.getIsQuery() != null)
			sql.append(" and kam.btwogvisible = '"+dto.getIsQuery()+"' ");
		
		
		//按deptid查询大客户
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			for(int i = 0; i < deptList.size(); i++) {
				if(i == 0) 
					sql.append(" and kam.departmentid in ( " + deptList.get(i).getME_departmentid());
				else
					sql.append( " , " + deptList.get(i).getME_departmentid());
			}
			sql.append(") ");
		}else {
			return new ArrayList();
		}
		
		//如果是客户经理
		String managerName = contractservice.getManagerName(accountId, roleId, groupId);
		if(managerName != null) {
			List<Long> kamids = getKamByManagerId(managerName);
			for(int i = 0; i < kamids.size(); i++) {
				if(i == 0)
					sql.append(" and kam.kamid in ( " + kamids.get(i));
				else
					sql.append(" , " + kamids.get(i));
			}
			sql.append(") ") ;
		}
		sql.append(" order by kam.kamnamecn ");
		List<Object[]> objs = getEntityManager().queryForListByHibernateSql(sql.toString(),p,null);
		TKam kam = null;
		for(Object[] obj : objs) {
			if(obj[1] != null && obj[2] != null){
				kam = new TKam();
				kam.setKamid(((BigDecimal)obj[0]).longValue());
				kam.setKamno(obj[1].toString());
				kam.setKamnamecn(obj[2].toString());
				if(obj[3] != null)
					kam.setKamnameen(obj[3].toString());
				if(obj[4] != null)
					kam.setBtwogVisible(Long.parseLong(obj[4].toString()));
				list.add(kam);
			}
		}
		return list;
	}	
	
	public String queryTkamIdsSql(Long groupId, Long roleId, Long accountId) throws ServiceException{
		StringBuffer sql = new StringBuffer(" select kam.kamno from TKam kam " )
				.append(" where kam.delflag = 0 and kam.status > 2 and kam.status != 11 ");
		
		//按deptid查询大客户
		List<TPriDeptCM> deptList = this.getDeptByGroup(groupId);
		if(null != deptList && deptList.size() != 0) {
			for(int i = 0; i < deptList.size(); i++) {
				if(i == 0) 
					sql.append(" and kam.TPriDept.departmentid in ( " + deptList.get(i).getME_departmentid());
				else
					sql.append( " , " + deptList.get(i).getME_departmentid());
			}
			sql.append(") ");
		}else {
			return null;
		}
		
		//如果是客户经理
		String managerName = contractservice.getManagerName(accountId, roleId, groupId);
		if(managerName != null) {
			List<Long> kamids = getKamByManagerId(managerName);
			for(int i = 0; i < kamids.size(); i++) {
				if(i == 0)
					sql.append(" and kam.kamid in ( " + kamids.get(i));
				else
					sql.append(" , " + kamids.get(i));
			}
			sql.append(") ") ;
		}
		
		return sql.toString();
	}

	//查询大某一客户对应的特殊航线
	@SuppressWarnings("unchecked")
	public List<TSpecialAirline> querySpecialList(Long kamid){
		String hql = "from TSpecialAirline air where air.TKam.kamid="+kamid;
		List<TSpecialAirline> list = getEntityManager().queryForListByHql(hql);
		
		return list;
	}
	
	//查询某一个特殊航线的舱位折扣
	@SuppressWarnings("unchecked")
	public List<TSpaceDiscount> querySpaceList(Long specialAirlineId){

		String hql2 = "from TSpaceDiscount dis where dis.TSpecialAirline.specialAirlineId="+specialAirlineId;
		List<TSpaceDiscount> spaceDiscounts = getEntityManager().queryForListByHql(hql2);
		return spaceDiscounts;
	}
	
	/**
	 * 每天定期调用webservice发送增量数据
	 */
	public void doSyncKamsTocc(){

        // 得到增量数据的客户编号，合同ID
        List<Object[]> suitList = getSuitList();

        // 循环调用webservice
        for (Object[] val : suitList) {
            String kamNo = val[0].toString();
            doSyncKamTocc(kamNo);
        }

    }

	public void doSyncKamTocc(String kamNo) {
        String[] cabins = { "F", "C", "Y", "P", "J", "Z", "K", "B", "E", "H","L", "M" };

        try {
            com.cea.callcenter.ws.kmsnew.ObjectFactory of = new com.cea.callcenter.ws.kmsnew.ObjectFactory();
            KmsGroupSyncNewImplService client = new KmsGroupSyncNewImplService();
            KmsGroupSyncNew service = client.getKmsGroupSyncNewImplPort();
            GroupKamInfoNew groupKamInfoNew = of.createGroupKamInfoNew();
            List<KamPolicyNew> kamPolicyNewList = new ArrayList<KamPolicyNew>();
            List<SpecialAirlineDiscountNew> specialAirlineDiscountNewList = new ArrayList<SpecialAirlineDiscountNew>();
            List<AddServiceNew> addServiceNewList = new ArrayList<AddServiceNew>();

            // 客户信息
            TKam tkam = (TKam)super.getEntityManager().queryForObjectByHql("from TKam where kamno = ? and delflag = '0'", new Object[]{kamNo});
            if (tkam != null) {
                // 设置GroupKamInfoNew
                if (tkam.getAgreementstatus() != null) {
                    groupKamInfoNew.setAgreementStatus(Long.valueOf(tkam
                            .getAgreementstatus()));
                }
                if (tkam.getAgreementtype() != null) {
                    groupKamInfoNew.setAgreementType(Long.valueOf(tkam
                            .getAgreementtype()));
                }
                if (tkam.getCompanytype() != null) {
                    groupKamInfoNew.setCompanyType(Long.valueOf(tkam
                            .getCompanytype()));
                }
                if (tkam.getDelflag() != null) {
                    groupKamInfoNew.setDelflag(tkam
                            .getDelflag().intValue());
                }
                if (tkam.getEmployeenumber() != null) {
                    groupKamInfoNew.setEmployeeNumber(Long.valueOf(tkam
                            .getEmployeenumber()));
                }
                if (tkam.getKamcategory() != null) {
                    groupKamInfoNew.setKamCategory(Long.valueOf(tkam
                            .getKamcategory()));
                }
                if (tkam.getKamcontinue() != null) {
                    groupKamInfoNew.setKamContinue(Long.valueOf(tkam
                            .getKamcontinue()));
                }
                if (tkam.getKamrank() != null) {
                    groupKamInfoNew.setKamRank(Long.valueOf(tkam
                            .getKamrank()));
                }
                if (tkam.getStatus() != null) {
                    groupKamInfoNew.setStatus(Long.valueOf(tkam
                            .getStatus()));
                }
                if (tkam.getAcceptdate() != null) {
                    String dateStr = DateUtils.getFormatDate(tkam
                            .getAcceptdate(), "yyyyMMdd");
                    groupKamInfoNew.setAcceptDate(XMLGregorianCalendarImpl
                            .parse(dateStr));
                }
                if (tkam.getApplydate() != null) {
                    String dateStr = DateUtils.getFormatDate(tkam
                            .getApplydate(), "yyyyMMdd");
                    groupKamInfoNew.setApplyDate(XMLGregorianCalendarImpl
                            .parse(dateStr));
                }
                if (tkam.getRegistdate() != null) {
                    String dateStr = DateUtils.getFormatDate(tkam
                            .getRegistdate(), "yyyyMMdd");
                    groupKamInfoNew.setRegistDate(XMLGregorianCalendarImpl
                            .parse(dateStr));
                }
                if (tkam.getRejectdate() != null) {
                    String dateStr = DateUtils.getFormatDate(tkam
                            .getRejectdate(), "yyyyMMdd");
                    groupKamInfoNew.setRejectDate(XMLGregorianCalendarImpl
                            .parse(dateStr));
                }
                groupKamInfoNew.setKamId(tkam.getKamid());
                groupKamInfoNew.setAddress(tkam.getAddress());
                groupKamInfoNew.setFax(tkam.getFax());
                groupKamInfoNew.setKamNameCn(tkam.getKamnamecn());
                groupKamInfoNew.setKamNameEn(tkam.getKamnameen());
                groupKamInfoNew.setKamNo(tkam.getKamno());
                groupKamInfoNew.setPostcode(tkam.getPostcode());
                groupKamInfoNew.setRemark(tkam.getRemark());

                // 设置KamPolicyNew
                String sql1 = "SELECT " 
                        + "客户ID, " 
                        + "舱位, " 
                        + "舱位类型, "
                        + "折扣OR价格, " 
                        + "FAREBASIS, " 
                        + "TOURCODE, "
                        + "航线限制适用不适用开关, " 
                        + "航线限制, " 
                        + "航班限制适用不适用开关, "
                        + "航班限制, " 
                        + "退票规定, " 
                        + "改期规定, " 
                        + "签转规定, "
                        + "周中周末限制, " 
                        + "停留限制, " 
                        + "Z值规定, " 
                        + "EI项, "
                        + "允许组合运价, " 
                        + "适用代码共享航班, " 
                        + "适用分子公司, "
                        + "SALETIMESTART, " 
                        + "SALETIMEEND, "
                        + "TRAVLETIMESTART, " 
                        + "TRAVLETIMEEND, "
                        + "VALIDATEDAT, " 
                        + "INVALIDATEDATE "
                        + "FROM KAM_POLICY_VIEW v WHERE v.客户编号 = '" + kamNo + "'";
                
                List<Object[]> kamPolicyViewList = getEntityManager()
                        .queryForListBySql(sql1);

                if (!CollectionUtils.isEmpty(kamPolicyViewList)) {
                    for (Object[] obj : kamPolicyViewList) {
                        KamPolicyNew kamPolicyNew = of.createKamPolicyNew();
                        kamPolicyNew.setKamId(Long.valueOf(obj[0].toString()));
                        kamPolicyNew.setClassCode(obj[1] == null ? "" : obj[1]
                                .toString());
                        kamPolicyNew.setClassType(obj[2] == null ? "" : obj[2]
                                .toString());
                        kamPolicyNew.setDiscount(obj[3] == null ? "" : obj[3]
                                .toString());
                        kamPolicyNew.setFareBasis(obj[4] == null ? "" : obj[4]
                                .toString());
                        kamPolicyNew.setTourCode(obj[5] == null ? "" : obj[6]
                                .toString());
                        kamPolicyNew.setIfLineLimit(obj[6] == null ? ""
                                : obj[6].toString());
                        kamPolicyNew.setLineLimit(obj[7] == null ? "" : obj[7]
                                .toString());
                        kamPolicyNew.setIfFlightLimit(obj[8] == null ? ""
                                : obj[8].toString());
                        kamPolicyNew.setFlightLimit(obj[9] == null ? ""
                                : obj[9].toString());
                        kamPolicyNew.setReturnRule(obj[10] == null ? ""
                                : obj[10].toString());
                        kamPolicyNew.setChangeRule(obj[11] == null ? ""
                                : obj[11].toString());
                        kamPolicyNew.setSignturnRule(obj[12] == null ? ""
                                : obj[12].toString());
                        kamPolicyNew.setWeekendLimit(obj[13] == null ? ""
                                : obj[13].toString());
                        kamPolicyNew.setStayLimit(obj[14] == null ? ""
                                : obj[14].toString());
                        kamPolicyNew.setHasz(obj[15] == null ? "" : obj[15]
                                .toString());
                        kamPolicyNew.setEi(obj[16] == null ? "" : obj[16]
                                .toString());
                        kamPolicyNew.setCombinationFares(obj[17] == null ? ""
                                : obj[17].toString());
                        kamPolicyNew.setShareCode(obj[18] == null ? ""
                                : obj[18].toString());
                        kamPolicyNew.setSubcompany(obj[19] == null ? ""
                                : obj[19].toString());

                        if (obj[20] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[20], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setSaleTimeStart(dateStr);
                        }
                        if (obj[21] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[21], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setSaleTimeEnd(dateStr);
                        }
                        if (obj[22] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[22], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setTravelTimeStart(dateStr);
                        }
                        if (obj[23] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[23], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setTravelTimeEnd(dateStr);
                        }
                        if (obj[24] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[24], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setValidateDate(dateStr);
                        }
                        if (obj[25] != null) {
                            String dateStr = DateUtils.getFormatDate(
                                    (Date) obj[25], SystemConfiguration.getDefaultDateTimeFormat());
                            kamPolicyNew
                                    .setInvalidateDate(dateStr);
                        }
                        // TODO
                        // kamPolicyNew.setFlightSeg(String);
                        // kamPolicyNew.setReturnLimit(String);
                        // kamPolicyNew.setSubject(String);

                        kamPolicyNewList.add(kamPolicyNew);
                    }
                }

                // 设置SpecialAirlineDiscountNew
                // *****TSpecialAirline和TSpaceDiscount平行设置*****
                List<TSpecialAirline> specialAirlineList = this
                        .querySpecialList(tkam.getKamid());
                if (!CollectionUtils.isEmpty(specialAirlineList)) {
                    for (TSpecialAirline airline : specialAirlineList) {
                        String hql2 = "from TSpaceDiscount dis where dis.TSpecialAirline.specialAirlineId=? order by dis.spaceDiscountId";
                        List<TSpaceDiscount> spaceDiscounts = getEntityManager()
                                .queryForListByHql(hql2, new Object[] { airline.getSpecialAirlineId() });
                        if (!CollectionUtils.isEmpty(spaceDiscounts)) {
                            for (int i = 0; i < spaceDiscounts.size(); i++) {
                                SpecialAirlineDiscountNew specialAirlineDiscountNew = of
                                        .createSpecialAirlineDiscountNew();
                                specialAirlineDiscountNew.setAirlineEnd(airline
                                        .getAirlineEnd());
                                specialAirlineDiscountNew
                                        .setAirlineStart(airline
                                                .getAirlineStart());

                                String cabin = "";
                                if ("true".equals(spaceDiscounts.get(i)
                                        .getSpaceType())) {
                                    cabin = cabins[i];
                                }
                                specialAirlineDiscountNew.setCabin(cabin);
                                specialAirlineDiscountNew.setKamid(tkam.getKamid());
                                specialAirlineDiscountNew.setRangeType(airline
                                        .getRangeType());
                                specialAirlineDiscountNew
                                        .setSpecialAirlineId(airline
                                                .getSpecialAirlineId());
                                specialAirlineDiscountNew
                                        .setSpaceDiscount(spaceDiscounts.get(i)
                                                .getSpaceDiscount() == null ? ""
                                                : spaceDiscounts.get(i)
                                                        .getSpaceDiscount()
                                                        .toString());
                                specialAirlineDiscountNew
                                        .setSpecialDiscountId(spaceDiscounts
                                                .get(i).getSpaceDiscountId());

                                specialAirlineDiscountNewList
                                        .add(specialAirlineDiscountNew);
                            }
                        }
                    }
                }
                
                // 设置AddServiceNew
                List<ValueAddedService> valueAddedServices = this
                        .queryValueByKam(tkam.getKamid());
                StringBuilder sb = new StringBuilder();
                if (!CollectionUtils.isEmpty(valueAddedServices)) {
                    for (ValueAddedService addService : valueAddedServices) {
                        AddServiceNew addServiceNew = of.createAddServiceNew();
                        addServiceNew.setAirport(addService.getAirport());
                        addServiceNew.setCabinAvailable(addService
                                .getCabinvaliable());
                        addServiceNew.setCabinInavailable(addService
                                .getCabininvaliable());
                        addServiceNew.setRemark(addService.getRemark());
                        addServiceNew.setServiceId(addService.getServiceid());
                        addServiceNew.setServiceNameCn(addService
                                .getServicenamecn());
                        addServiceNew.setServiceNameEn(addService
                                .getServicenameen());
                        addServiceNew.setServiceType(addService
                                .getServicetype());
                        addServiceNew.setSupplierName(addService
                                .getSuppliername());
                        sb.append(addService.getServiceid()).append(",");

                        addServiceNewList.add(addServiceNew);
                    }
                }

                if (sb.length() > 0) {
                    groupKamInfoNew.setServiceId(sb.toString().substring(0,
                            sb.length() - 1));
                }

                boolean fanhui = service.updateGroupInfo(groupKamInfoNew,
                        kamPolicyNewList, specialAirlineDiscountNewList,
                        addServiceNewList);
                // TODO
                //boolean fanhui = true;

                Date currentTime = getEntityManager().getDbTime();

                TKamLog tkamlog = new TKamLog();

                tkamlog.setTKam(tkam);
                tkamlog.setOperatorid(tkam.getOperateid());
                tkamlog.setOperatedate(currentTime);
                //101代表更新
                tkamlog.setOperatetype(new Long(101));
                tkamlog.setRemark("同步" + tkam.getKamno() + "-"
                        + tkam.getKamnamecn() + "到callcenter: " + fanhui);
                getEntityManager().save(tkamlog);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (e != null && e.toString().length() > 3999)
                logfaild.setRemark(e.toString().substring(0, 3999));
        }
    }

	/**
     * 得到增量数据的客户编号，合同ID
     * 
     * @return
     */
    private List<Object[]> getSuitList() {
        String sql = "SELECT 客户编号,CONTRACTID FROM KAM_POLICY_VIEW GROUP BY 客户编号,CONTRACTID ORDER BY 客户编号,CONTRACTID ";
        // 拿到更新前数据
        List<Object[]> listBef = getEntityManager().queryForListBySql(sql);

        // 调用procedure
        getEntityManager().executeProcedure("{call changeContractStatus()}",
                new CallableStatementCallback() {
                    public Boolean doInCallableStatement(
                            final CallableStatement cs) throws SQLException,
                            DataAccessException {
                        cs.execute();
                        return true;
                    }
                });
        hibernate.flush();
        
        // 拿到更新后数据
        List<Object[]> listAft = getEntityManager().queryForListBySql(sql);
        
        List<Object[]> suitList = new ArrayList<Object[]>();
        List<Object[]> removeList = new ArrayList<Object[]>();

        // 比较得到增量数据
        for (Object[] bef : listBef) {
            String kamNoBef = bef[0].toString();
            String contractIdBef = bef[1].toString();
            
            boolean hasKam = false;

            for (Object[] aft : listAft) {
                String kamNoAft = aft[0].toString();
                String contractIdAft = aft[1].toString();

                // 客户编号,合同ID都没变
                if (kamNoBef.equals(kamNoAft)
                        && contractIdBef.equals(contractIdAft)) {
                    hasKam = true;
                    removeList.add(aft);
                    break;
                }
                // 客户编号没变,合同ID变
                else if (kamNoBef.equals(kamNoAft)
                        && !contractIdBef.equals(contractIdAft)) {
                    hasKam = true;
                    suitList.add(aft);
                    removeList.add(aft);
                    break;
                }
            }
            // 原来有，现在没
            if (!hasKam) {
                suitList.add(new Object[] { kamNoBef, "" });
            }
        }
        
        // 在更新后数据中去除所有更改过的数据，得到新增数据
        listAft.removeAll(removeList);
        suitList.addAll(listAft);
        
        return suitList;
    }
    
    public void doUpdateKamsToCC() throws ServiceException{
        List<String> retList = new ArrayList<String>();
        String sql = "select kamno from t_kam where delflag = '0' group by kamno";
        retList = getEntityManager().queryForListBySql(sql);
        for(String kamNo : retList){
            this.doSyncKamTocc(kamNo);
        }
    }

	@SuppressWarnings("unchecked")
	public List<TSubCompany> querySubcompByKamnoWithPage(String kamno,OrderablePagination p) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TSubCompany.class);
		bqc.addExpressions(Restrictions.equal("kamNo", kamno));
		bqc.addExpressions(Restrictions.equal("status", "0"));
		List<TSubCompany> subcompList = getEntityManager().queryWithPagination(bqc,p);
		return subcompList;
	}

	public void synSubcompFromB2g() throws Exception {
		Date nowDt = new Date();
		List<BasePo> saveList = new ArrayList<BasePo>();
		List<BasePo> updateList = new ArrayList<BasePo>();
		
		List<TrsubcompB2g> b2gList = this.queryB2gSubcomp();
		for(TrsubcompB2g b2g : b2gList){
			BeanQueryCondition bqc = BeanQueryCondition.forClass(TSubCompany.class);
			bqc.addExpressions(Restrictions.equal("b2gId", b2g.getId()));
			TSubCompany subcomp = (TSubCompany) getEntityManager().queryForObject(bqc);
			if(subcomp==null){
				TSubCompany com  = new TSubCompany();
				com.setB2gId(b2g.getId());
				com.setCompAddr(b2g.getCompAddr());
				com.setCompNm(b2g.getCompNm());
				com.setContactor(b2g.getContactor());
				com.setEmail(b2g.getEmail());
				com.setHeadCompCd(b2g.getHeadCompCd());
				com.setKamNo(b2g.getKamNo());
				com.setSubCompCd(b2g.getSubCompCd());
				com.setTele(b2g.getTele());
				com.setUpdateDt(nowDt);
				com.setCreateDt(nowDt);
				com.setStatus(b2g.getStatus());
				saveList.add(com);
			}else{
				subcomp.setB2gId(b2g.getId());
				subcomp.setCompAddr(b2g.getCompAddr());
				subcomp.setCompNm(b2g.getCompNm());
				subcomp.setContactor(b2g.getContactor());
				subcomp.setEmail(b2g.getEmail());
				subcomp.setHeadCompCd(b2g.getHeadCompCd());
				subcomp.setKamNo(b2g.getKamNo());
				subcomp.setSubCompCd(b2g.getSubCompCd());
				subcomp.setTele(b2g.getTele());
				subcomp.setUpdateDt(nowDt);
				subcomp.setStatus(b2g.getStatus());
				updateList.add(subcomp);
			}
		}
		getEntityManager().save(saveList);
		getEntityManager().update(updateList);

	}
	
	/***
	 * 获取B2G中新增的记录
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private List<TrsubcompB2g> queryB2gSubcomp() throws Exception{
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TrsubcompB2g.class);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal=Calendar.getInstance();
		String endDt = sdf.format(cal.getTime());
		cal.add(Calendar.DATE,-1);
        String bngDt = sdf.format(cal.getTime());
        bqc.addExpressions(Restrictions.between("lastModDt", sdf.parse(bngDt), sdf.parse(endDt)));
		List<TrsubcompB2g> b2gList = getB2bEntityManager().query(bqc);
		return b2gList;
	}

	public TSubCompany querySubcompById(String id) {
		TSubCompany comp = getEntityManager().get(TSubCompany.class, Long.parseLong(id));
		return comp;
	}

	@SuppressWarnings("unchecked")
	public List<TSubCompany> querySubcompByKamno(String kamno,String adminTp,String subcompCd) {
		BeanQueryCondition bqc = BeanQueryCondition.forClass(TSubCompany.class);
		bqc.addExpressions(Restrictions.equal("kamNo", kamno));
		bqc.addExpressions(Restrictions.equal("status", "0"));
		if(StringUtils.hasText(adminTp) && ADMIN_TP_S.equals(adminTp)){
			bqc.addExpressions(Restrictions.equal("subCompCd", subcompCd));
		}
		List<TSubCompany> subcompList = getEntityManager().query(bqc);
		if(StringUtils.hasText(adminTp) && ADMIN_TP_T.equals(adminTp)){
			TSubCompany subcomp = new TSubCompany();
			subcomp.setSubCompCd("00");
			subcomp.setCompNm("总公司");
			subcompList.add(subcomp);
		}
		return subcompList;
	}
	
	public List<String> queryTKamContract(String str){
		  List<String> retList = new ArrayList<String>();
		  String sql="select to_char(MAX(substr(t.contractno,-3,3))+1) from t_kam_contract t where  length(t.contractno)=11 and t.contractno like 'SHA%' and substr(t.contractno,-4,1)='D' and substr(t.contractno,4,4)='"+str+"'";
		  return getEntityManager().queryForListBySql(sql);
	}
	
	public String quertMaxTKam(){
		String sql="select to_char(max(substr(t.kamno,4,3))+1) from t_kam t where t.kamno like '909%' and t.kamno <> '9099993' and t.kamno not like '909090%' and (t.delflag<>1 or t.delflag is null)";
		String kam_909 = getEntityManager().queryForStringBySql(sql);
		if("90".equals(kam_909))
			kam_909 = "91";
//		StringBuffer sb = new StringBuffer();
//		if (kam_909.length() < 3) {
			   while (kam_909.length() < 3) {
				   kam_909 = "0"+kam_909;
//				   sb.append("0").append(kam_909);//左补0
//			    sb.append(str).append("0");//右补0
			   }
//		} 
		kam_909 = "9"+kam_909;
		return kam_909;
	}
	
	private static final String ADMIN_TP_T = "T";//总公司管理员
	private static final String ADMIN_TP_S = "S";//子公司管理员

}
