using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using TNGS.NetRoutines;
using Unlz.Interfaces;
using System.Data.SqlClient;
using System.Data;
using System.Data.SqlTypes;
using System.Configuration;

namespace Unlz.FileProcess
{
    public class FormatoInscripcion : IProceso
    {
        #region Objects

        private SqlConnection bdConnection;
        private SqlTransaction spTransaction;
        private const String sp_ImportInscripcion = "InscripcionInsert";
        private const String sp_TurnoInscripcionCleanData = "ServicioSelectTurnoInscripcionCleanData";
        private const String sp_DeleteInscripciones = "InscripcionDeleteAllByTurnoInscripcion_IdVuelta";
        private const String sp_ValidateUser = "UsuarioSelect";
        private const String sp_ImportPadron = "UsuarioImportPadron";
        private const String IdTipoInscripcionPromocion = "P";
        private DateTime turno;
        private int vuelta;

        #endregion

        /// <summary>
        /// Devuelve el formato del registro
        /// </summary>
        public ArrayList GetFormat()
        {
            // No hay formato
            return null;
        }

        /// <summary>
        /// Devuelve el delimitador de campos
        /// </summary>
        public string GetDelimiter()
        {
            // Devolvemos el delimitador
            return ";";
        }

        /// <summary>
        /// Incia el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_strFileName">Nombre del archivo a procesar</param>
        /// <param name="p_strExtraData">Datos extras asociados</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void Init(string p_strFileName, string p_strExtraData, ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoInscripcion", "Init");

            try
            {
                this.bdConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["InscripcionesCursos"].ConnectionString);
                this.bdConnection.Open();
                this.spTransaction = bdConnection.BeginTransaction("TransactionInscripcion");
                CleanInscriptions(p_smResult);
            }
            catch (Exception l_expData)
            {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Limpia la tabla de Inscripciones del turno actvio
        /// </summary>
        /// <param name="p_smResult">Estado final de la operacion</param>
        private void CleanInscriptions(StatMsg p_smResult)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(sp_TurnoInscripcionCleanData, bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Transaction = this.spTransaction;
                    var result = cmd.ExecuteReader();

                    result.Read();
                    if (result.HasRows)
                    {
                        turno = result.GetDateTime(0);
                        vuelta = result.GetInt32(1);
                    }
                    result.Close();
                }

                if (turno != null && vuelta != null)
                {
                    using (SqlCommand cmd = new SqlCommand(sp_DeleteInscripciones, bdConnection))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add("@TurnoInscripcion", SqlDbType.DateTime).Value = turno;
                        cmd.Parameters.Add("@IdVuelta", SqlDbType.Int).Value = vuelta;

                        cmd.Transaction = this.spTransaction;
                        cmd.ExecuteScalar();
                    }
                }
            }
            catch (Exception l_expData)
            {
                p_smResult.BllError(l_expData.ToString());
            }
        }

        /// <summary>
        /// Procesa los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_iNroRec">Numero de registro</param>
        /// <param name="p_astrData">Datos del registro a procesar</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void Process(int p_iNroRec, string[] p_astrData, ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoInscripcion", "Process");

            try
            {
                #region Validations
                double numCheck;
                DateTime dateCheck;

                //VALIDA TIPO INSCRIPCION
                if (p_astrData[0].Trim().Length == 0)
                {
                    p_smResult.BllError("El Tipo de Inscripcion debe contener un valor.");
                    return;
                }

                //VALIDA TURNO DE INSCRIPCION
                if (p_astrData[1].Trim().Length == 0)
                {
                    p_smResult.BllError("El Turno de Inscripcion debe contener un valor.");
                    return;
                }
                else
                {
                    if (!DateTime.TryParse(p_astrData[1], out dateCheck))
                    {
                        p_smResult.BllError("El Turno de Inscripcion debe ser del tipo DateTime.");
                        return;
                    }
                }

                //VALIDA ID_VUELTA
                if (p_astrData[2].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Vuelta debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[2], out numCheck))
                    {
                        p_smResult.BllError("El Id de Vuelta debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA ID_MATERIA
                if (p_astrData[3].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Materia debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[3], out numCheck))
                    {
                        p_smResult.BllError("El Id de Materia debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA CATEDRA
                if (p_astrData[4].Trim().Length == 0)
                {
                    p_smResult.BllError("La Catedra/Comision debe contener un valor.");
                    return;
                }

                //VALIDA DNI
                if (p_astrData[5].Trim().Length == 0)
                {
                    p_smResult.BllError("El DNI debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[5], out numCheck))
                    {
                        p_smResult.BllError("El DNI debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA ORIGEN_INSCRIPCION              
                if (p_astrData[6].Trim().Length == 0)
                {
                    p_smResult.BllError("El Origen de Inscripcion debe contener un valor.");
                    return;
                }

                //VALIDA FECHA_ALTA_INSCRI¨CION
                if (p_astrData[8].Trim().Length == 0)
                {
                    p_smResult.BllError("La Fecha de Alta de Inscripcion debe contener un valor.");
                    return;
                }
                else
                {
                    if (!DateTime.TryParse(p_astrData[8] + " " + p_astrData[9], out dateCheck))
                    {
                        p_smResult.BllError("La Fecha de Alta de Inscripcion debe ser del tipo DateTime.");
                        return;
                    }
                }

                //VALIDA FECHA_MODIFICACION_INSCRIPCION
                if (p_astrData[11].Trim().Length > 0)
                {
                    if (!DateTime.TryParse(p_astrData[11] + " " + p_astrData[12], out dateCheck))
                    {
                        p_smResult.BllError("La Fecha de Alta de Inscripcion debe ser del tipo DateTime.");
                        return;
                    }
                }
                #endregion

                using (SqlCommand cmd = new SqlCommand(sp_ImportInscripcion, this.bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    if (!ValidateStudentsInPadron(Convert.ToInt32(p_astrData[5]), p_smResult))
                        InsertMissedUser(Convert.ToInt32(p_astrData[5]), p_smResult);
                    
                    cmd.Parameters.Add("@IdTipoInscripcion", SqlDbType.Char).Value = p_astrData[0];
                    cmd.Parameters.Add("@TurnoInscripcion", SqlDbType.Date).Value = Convert.ToDateTime(p_astrData[1]);
                    cmd.Parameters.Add("@IdVuelta", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[2]);
                    cmd.Parameters.Add("@IdMateria", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[3]);
                    cmd.Parameters.Add("@CatedraComision", SqlDbType.VarChar).Value = p_astrData[4];
                    cmd.Parameters.Add("@DNI", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[5]);
                    cmd.Parameters.Add("@IdEstadoInscripcion", SqlDbType.Char).Value = p_astrData[6].Trim();
                    cmd.Parameters.Add("@OrigenInscripcion", SqlDbType.Char).Value = ((Object)p_astrData[5].Trim() ?? DBNull.Value);
                    cmd.Parameters.Add("@FechaAltaInscripcion", SqlDbType.DateTime).Value = p_astrData[8].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[8] + " " + p_astrData[9]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@OrigenModificacion", SqlDbType.Char).Value = ((Object)p_astrData[5].Trim() ?? DBNull.Value);
                    cmd.Parameters.Add("@FechaModificacionInscripcion", SqlDbType.DateTime).Value = p_astrData[11].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[11] + " " + p_astrData[12]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@DNIEmpleadoAlta", SqlDbType.Int).Value = p_astrData[13].Trim().Length > 0 ? Convert.ToInt32(p_astrData[13]) : 0;
                    cmd.Parameters.Add("@DNIEmpleadoMod", SqlDbType.Int).Value = p_astrData[14].Trim().Length > 0 ? Convert.ToInt32(p_astrData[14]) : 0;

                    cmd.Transaction = this.spTransaction;
                    cmd.ExecuteNonQuery();
                }

            }
            catch (Exception l_expData)
            {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Inserta al alumno si no existe en la BD
        /// </summary>
        /// <param name="dni">DNI del alumno</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        private void InsertMissedUser(int dni, StatMsg p_smResult)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(sp_ImportPadron, this.bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@DNI", SqlDbType.Int).Value = dni;
                    cmd.Parameters.Add("@ApellidoNombre", SqlDbType.VarChar).Value = "Sin Datos";
                    cmd.Parameters.Add("@IdSede", SqlDbType.Int).Value = -1;
                    cmd.Parameters.Add("@IdEstado", SqlDbType.Char).Value = DBNull.Value;
                    cmd.Parameters.Add("@IdCarrera", SqlDbType.Int).Value = DBNull.Value;
                    cmd.Parameters.Add("@CuatrimestreAnioIngreso", SqlDbType.VarChar).Value = DBNull.Value;
                    cmd.Parameters.Add("@CuatrimestreAnioReincorporacion", SqlDbType.VarChar).Value = DBNull.Value;
                    cmd.Parameters.Add("@IdCargo", SqlDbType.Int).Value = 2;

                    cmd.Transaction = this.spTransaction;
                    var result = cmd.ExecuteReader();
                    result.Close();
                }
            }
            catch (Exception l_expData)
            {
                p_smResult.BllError(l_expData.ToString());
            }
        }

        /// <summary>
        /// Valida si existe el DNI del alumno
        /// </summary>
        /// <param name="dni">DNI del alumno</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        private bool ValidateStudentsInPadron(int dni, StatMsg p_smResult)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(sp_ValidateUser, this.bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@DNI", SqlDbType.Int).Value = dni;
                    
                    cmd.Transaction = this.spTransaction;
                    var result = cmd.ExecuteReader();

                    if (result.HasRows)
                    {
                        result.Close();
                        return true;
                    }
                    else
                    {
                        result.Close();
                        return false;
                    }
                }
            }
            catch (Exception l_expData)
            {
                p_smResult.BllError(l_expData.ToString());
                return false;
            }
        }

        /// <summary>
        /// Finaliza el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void End(ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoInscripcion", "End");

            try
            {
                if (spTransaction != null)
                    spTransaction.Commit();

                if (this.bdConnection != null) {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception l_expData)
            {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Indica que se aborto el proceso por problemas en la capa de proceso
        /// </summary>
        /// <param name="p_smResult">Estado de error de la operacion</param>
        public void Abort(StatMsg p_smResult)
        {
            try
            {
                // Rollback de la transaccion
                if (spTransaction != null)
                    spTransaction.Rollback();

                if (this.bdConnection != null) {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception) {
            }
        }
    }
}
