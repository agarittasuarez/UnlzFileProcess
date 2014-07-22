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
    public class FormatoCatedra : IProceso
    {
        #region Objects

        private SqlConnection bdConnection;
        private SqlTransaction spTransaction;
        private const String sp_ImportCatedras = "CatedraComisionInsert";
        private const String IdTipoInscripcionPromocion = "P";

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
            p_smResult.BllReset("FormatoCatedra", "Init");

            try
            {
                this.bdConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["InscripcionesCursos"].ConnectionString);
                this.bdConnection.Open();
                this.spTransaction = bdConnection.BeginTransaction("TransactionCatedras");
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
            p_smResult.BllReset("FormatoCatedra", "Process");

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

                //VALIDA FECHAS
                if (p_astrData[5].Trim().Length > 0)
                {
                    if (!DateTime.TryParse(p_astrData[5], out dateCheck))
                    {
                        p_smResult.BllError("La FechaDesde debe ser del tipo DateTime.");
                        return;
                    }
                }

                if (p_astrData[6].Trim().Length > 0)
                {
                    if (!DateTime.TryParse(p_astrData[6], out dateCheck))
                    {
                        p_smResult.BllError("La FechaHasta debe ser del tipo DateTime.");
                        return;
                    }
                }

                //VALIDA ID_SEDE
                if (p_astrData[8].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Sede debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[8], out numCheck))
                    {
                        p_smResult.BllError("El Id de Sede debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA COMISION ABIERTA
                if (p_astrData[11].Trim().Length == 0)
                {
                    p_smResult.BllError("La Catedra debe definir si esta abierta o no.");
                    return;
                }
                #endregion

                using (SqlCommand cmd = new SqlCommand(sp_ImportCatedras, this.bdConnection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@IdTipoInscripcion", SqlDbType.Char).Value = p_astrData[0];
                    cmd.Parameters.Add("@TurnoInscripcion", SqlDbType.Date).Value = p_astrData[1].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[1]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@IdVuelta", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[2]);
                    cmd.Parameters.Add("@IdMateria", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[3]);

                    if(p_astrData[0] == IdTipoInscripcionPromocion)
                        cmd.Parameters.Add("@CatedraComision", SqlDbType.VarChar).Value = p_astrData[4].PadLeft(3, '0');
                    else
                        cmd.Parameters.Add("@CatedraComision", SqlDbType.VarChar).Value = p_astrData[4].PadLeft(3, ' ');

                    cmd.Parameters.Add("@FechaDesde", SqlDbType.Date).Value = p_astrData[5].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[5]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@FechaHasta", SqlDbType.Date).Value = p_astrData[6].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[6]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@Horario", SqlDbType.VarChar).Value = p_astrData[7].Trim();
                    cmd.Parameters.Add("@IdSede", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[8]);
                    cmd.Parameters.Add("@ProfesorNombreApellido", SqlDbType.VarChar).Value = p_astrData[9].Trim().Replace('�', 'Ñ');
                    cmd.Parameters.Add("@ProfesorJerarquia", SqlDbType.VarChar).Value = p_astrData[10].Trim();
                    cmd.Parameters.Add("@ComisionAbierta", SqlDbType.Char).Value = p_astrData[11].Trim();

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
            p_smResult.BllReset("FormatoCatedra", "End");

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
