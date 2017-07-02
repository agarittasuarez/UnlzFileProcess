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
    public class FormatoInicioInscripcion : IProceso
    {
        #region Objects

        private SqlConnection bdConnection;
        private SqlTransaction spTransaction;
        private const String sp_ImportInicioInscripcion = "InscripcionActivaInsert";

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
            p_smResult.BllReset("FormatoInicioInscripcion", "Init");

            try
            {
                this.bdConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["InscripcionesCursos"].ConnectionString);
                this.bdConnection.Open();
                this.spTransaction = bdConnection.BeginTransaction("TransactionInicioInscripcion");
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
        /// Procesa los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_iNroRec">Numero de registro</param>
        /// <param name="p_astrData">Datos del registro a procesar</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void Process(int p_iNroRec, string[] p_astrData, ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoInicioInscripcion", "Process");

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

                //VALIDA ID_sede
                if (p_astrData[5].Trim().Length == 0)
                {
                    p_smResult.BllError("La Sede debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[5], out numCheck))
                    {
                        p_smResult.BllError("La Sede debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA FECHAS
                if (p_astrData[3].Trim().Length > 0)
                {
                    if (!DateTime.TryParse(p_astrData[3], out dateCheck))
                    {
                        p_smResult.BllError("La FechaDesde debe ser del tipo DateTime.");
                        return;
                    }
                }

                if (p_astrData[4].Trim().Length > 0)
                {
                    if (!DateTime.TryParse(p_astrData[4], out dateCheck))
                    {
                        p_smResult.BllError("La FechaHasta debe ser del tipo DateTime.");
                        return;
                    }
                }
                #endregion

                using (SqlCommand cmd = new SqlCommand(sp_ImportInicioInscripcion, this.bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@IdTipoInscripcion", SqlDbType.Char).Value = p_astrData[0];
                    cmd.Parameters.Add("@TurnoInscripcion", SqlDbType.Date).Value = p_astrData[1].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[1]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@IdVuelta", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[2]);
                    cmd.Parameters.Add("@InscripcionFechaDesde", SqlDbType.DateTime).Value = p_astrData[3].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[3]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@InscripcionFechaHasta", SqlDbType.DateTime).Value = p_astrData[4].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[4]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@IdSede", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[5]);

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
        /// Finaliza el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void End(ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoInicioInscripcion", "End");

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
